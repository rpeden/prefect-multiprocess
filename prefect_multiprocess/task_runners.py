"""
A multiprocess task runner for Prefect.

Example:
    ```python
    import time

    from prefect import flow, task

    @task
    def shout(number):
        time.sleep(0.5)
        print(f"#{number}")

    @flow
    def count_to(highest_number):
        for number in range(highest_number):
            shout.submit(number)

    if __name__ == "__main__":
        count_to(10)

    # outputs
    #0
    #1
    #2
    #3
    #4
    #5
    #6
    #7
    #8
    #9
    ```

    Switching to a `MultiprocessTaskRunner`:
    ```python
    import time

    from prefect import flow, task
    from prefect_multiprocess import MultiprocessTaskRunner

    @task
    def shout(number):
        time.sleep(0.5)
        print(f"#{number}")

    @flow(task_runner=MultiprocessTaskRunner())
    def count_to(highest_number):
        for number in range(highest_number):
            shout.submit(number)

    if __name__ == "__main__":
        count_to(10)

    # outputs
    #3
    #7
    #2
    #6
    #4
    #0
    #1
    #5
    #8
    #9
    ```

Note that I/O-bound tasks will not benefit from this task runner, as it is designed for CPU-bound tasks. I/O-bound tasks should use the `ThreadPoolTaskRunner` instead.
MultiprocessTaskRunner will still work find with I/O-bound tasks, but it will not provide any performance benefits - in fact, it'll be a bit slower due to the overhead
of creating and managing multiple processes.
"""

import multiprocessing
import os
from concurrent.futures import ProcessPoolExecutor
from typing import (
    TYPE_CHECKING,
    Any,
    Coroutine,
    Dict,
    Iterable,
    Mapping,
    Optional,
    ParamSpec,
    Self,
    TypeVar,
    overload,
)
from uuid import UUID, uuid4

import anyio
import cloudpickle
import concurrent.futures
from prefect import Task
from prefect.context import serialize_context
from prefect.client.schemas.objects import TaskRunInput, State
from prefect.futures import PrefectFuture, PrefectFutureList, PrefectWrappedFuture
from prefect.task_runners import TaskRunner, ThreadPoolTaskRunner
from prefect.utilities.collections import visit_collection

from .utilities.remote import run_remote_task


if TYPE_CHECKING:
    from prefect.tasks import Task

P = ParamSpec("P")
R = TypeVar("R", covariant=True)
F = TypeVar("F", bound=PrefectFuture[Any])


class MultiprocessPrefectFuture[R](PrefectWrappedFuture[R, concurrent.futures.Future]):
    """
    A Prefect future that wraps an asyncio.Future. This future is used
    when the task run is submitted to a MultiprocessTaskRunner.
    """

    def wait(self, timeout: Optional[float] = None) -> None:
        result = None
        try:
            if timeout is not None:
                # asyncio futures do not support timeouts, so we
                # can improvise by using `anyio.fail_after`
                with anyio.fail_after(timeout):
                    result = cloudpickle.loads(self._wrapped_future.result())
            else:
                result = cloudpickle.loads(self._wrapped_future.result())

        except Exception:
            # It failed, or timed out. Either way, we're done waiting so we can GTFO.
            return
        if isinstance(result, State):
            self._final_state = result

    def result(
        self,
        timeout: Optional[float] = None,
        raise_on_failure: bool = True,
    ) -> R:
        if not self._final_state:
            future_result = None

            # Like `wait`, we can use `anyio.fail_after` to
            # implement a timeout for the result.
            if timeout is not None:
                # not going to bother catching the timeout exception
                # because we want that to propagate up the call stack
                with anyio.fail_after(timeout):
                    future_result = cloudpickle.loads(self._wrapped_future.result())
            else:
                # We'll wait forever if we have to!
                future_result = cloudpickle.loads(self._wrapped_future.result())

            if isinstance(future_result, State):
                self._final_state = future_result
            else:
                return future_result

        result = self._final_state.result(raise_on_failure=raise_on_failure)

        if isinstance(result, Exception):
            raise result
        else:
            return result


class MultiprocessTaskRunner(TaskRunner[MultiprocessPrefectFuture[R]]):
    """
    A parallel task_runner that submits tasks to uses Python's `multiprocessing`
    module to run tasks using multiple Python processes.
    Args:
        number_of_processes (int, optional): The number of processes to use for
        running tasks. If not provided, defaults to the number of CPUs/cores/threads on the
        host machine.
    Examples:

        Create one process for every CPU/CPU core/hyperthread
        ```python
        from prefect import flow
        from prefect_multiprocess.task_runners import MultiprocessTaskRunner


        @flow(task_runner=MultiprocessTaskRunner())
        def my_flow():
            ...
        ```

        Customizing the number of processes:
        ```python
        @flow(task_runner=MultiprocessTaskRunner(processes=4))
        def my_flow():
            ...
        ```
    """

    def __init__(self, processes: Optional[int] = None):
        super().__init__()
        self._process_count: Optional[int] = processes
        self._futures: Dict[UUID, concurrent.futures.Future[bytes]] = {}
        self._remote_task_runner: TaskRunner = ThreadPoolTaskRunner()

    def duplicate(self) -> "MultiprocessTaskRunner":
        """
        Returns a new instance of `MultiprocessTaskRunner` using the same settings
        as this instance.
        """

        return MultiprocessTaskRunner(processes=self._process_count)

    @overload
    def submit(
        self,
        task: "Task[P, Coroutine[Any, Any, R]]",
        parameters: Mapping[str, Any],
        wait_for: Iterable[PrefectFuture[Any]] | None = None,
        dependencies: Mapping[str, set[TaskRunInput]] | None = None,
    ) -> MultiprocessPrefectFuture[R]: ...

    @overload
    def submit(
        self,
        task: "Task[Any, R]",
        parameters: Mapping[str, Any],
        wait_for: Iterable[PrefectFuture[Any]] | None = None,
        dependencies: Mapping[str, set[TaskRunInput]] | None = None,
    ) -> MultiprocessPrefectFuture[R]: ...

    def submit(  # type: ignore[reportIncompatibleMethodOverride]
        self,
        task: Task[P, R],
        parameters: dict[str, Any],
        wait_for: Iterable[PrefectFuture[Any]] | None = None,
        dependencies: dict[str, set[TaskRunInput]] | None = None,
    ) -> MultiprocessPrefectFuture[R]:
        """
        Submits a task to the task runner.
        """

        if not self._started:
            raise RuntimeError(
                "The task runner must be started before submitting work."
            )

        if self._executor is None:
            raise RuntimeError(
                "Can't run tasks in a MultiprocessTaskRunner without an executor."
            )

        parameters = self._resolve_prefect_futures(parameters)
        wait_for = self._resolve_prefect_futures(wait_for) if wait_for else None

        task_run_id = uuid4()

        submit_kwargs: dict[str, Any] = dict(
            task_run_id=task_run_id,
            parameters=parameters,
            wait_for=wait_for,
            return_type="state",
            dependencies=dependencies,
        )

        context = serialize_context()
        # we don't want to serialize the task runner itself
        # because we don't want to spin up *another* multiprocess executor
        # in the child process, so we replace it with Prefect's default ThreadPoolTaskRunner.
        if context["flow_run_context"] and "flow" in context["flow_run_context"]:
            setattr(
                context["flow_run_context"]["flow"],
                "task_runner",
                self._remote_task_runner,
            )
        pickled_context = cloudpickle.dumps(context)

        task.__call__
        pickled_task = cloudpickle.dumps([task, submit_kwargs])
        future = self._executor.submit(run_remote_task, pickled_task, pickled_context)

        self._futures[task_run_id] = future

        return MultiprocessPrefectFuture[R](
            task_run_id=task_run_id, wrapped_future=future
        )

    @overload
    def map(
        self,
        task: "Task[P, Coroutine[Any, Any, R]]",
        parameters: dict[str, Any],
        wait_for: Iterable[PrefectFuture[Any]] | None = None,
    ) -> PrefectFutureList[MultiprocessPrefectFuture[R]]: ...

    @overload
    def map(
        self,
        task: "Task[Any, R]",
        parameters: dict[str, Any],
        wait_for: Iterable[PrefectFuture[Any]] | None = None,
    ) -> PrefectFutureList[MultiprocessPrefectFuture[R]]: ...

    def map(  # type: ignore[reportIncompatibleMethodOverride]
        self,
        task: "Task[P, R]",
        parameters: dict[str, Any],
        wait_for: Iterable[PrefectFuture[Any]] | None = None,
    ) -> PrefectFutureList[MultiprocessPrefectFuture[R]]:
        return super().map(task, parameters, wait_for)

    def cancel(self, key: UUID) -> None:
        """
        Cancels a task with the given key.
        """
        if key not in self._futures:
            raise KeyError(f"No task with key {key} is currently running.")

        future = self._futures[key]
        future.cancel()

        # Remove the cancelled future from the dictionary
        del self._futures[key]

    def cancel_all(self) -> None:
        """
        Cancels all tasks that are currently running.
        """

        for key in list(self._futures.keys()):
            self.cancel(key)

        # Clear the futures dictionary
        self._futures.clear()

        self._executor.shutdown(wait=False, cancel_futures=True)
        self._started = False

    def _resolve_prefect_futures(self, expr: Any):
        """
        Resolves any `MultiprocessPrefectFuture`s in task arguments
        before proceeding.
        """

        def visit(expr: Any) -> Any:
            if isinstance(expr, MultiprocessPrefectFuture):
                return expr.wrapped_future
            # Fallback to return the expression unaltered
            return expr

        return visit_collection(expr, visit_fn=visit, return_data=True)

        # Register the cloudpickle reducer for Prefect futures

    def __enter__(self) -> Self:
        """
        Start the task runner and prep for context exit.
        """
        # Use `spawn` to create processes, because forking breaks Prefect.
        super().__enter__()
        ctx = multiprocessing.get_context("spawn")
        process_count = (
            self._process_count if self._process_count is not None else os.cpu_count()
        )
        self._executor = ProcessPoolExecutor(max_workers=process_count, mp_context=ctx)
        self.logger.info(
            (f"Created a multiprocess task runner with {process_count} processes.")
        )
        # log process id
        self.logger.info(f"MultiprocessTaskRunner process ID: {os.getpid()}")
        return self

    def __eq__(self, other) -> bool:
        """
        Equality comparison to determine if two `MultiprocessTaskRunner`s are the same.
        """
        if type(self) != type(other):
            return False

        if type(other) is not MultiprocessTaskRunner:
            return False

        return self._process_count == other._process_count

    def __exit__(self, *args):
        """
        Cleans up shuts down the task runner.
        """
        self._executor.shutdown(wait=False, cancel_futures=True)
        self._started = False

    def __getstate__(self):
        """
        Allow the `MultiprocessTaskRunner` to be serialized by dropping the
        process pool.
        """
        data = self.__dict__.copy()
        data.update({k: None for k in {"_executor"}})
        return data

    def __setstate__(self, data: dict):
        """
        When deserialized, we will no longer have a reference to the process pool.
        """
        self.__dict__.update(data)
        self._pool = None
