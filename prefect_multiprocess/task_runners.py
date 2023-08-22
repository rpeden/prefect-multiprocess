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
"""
import asyncio
import multiprocessing as mp
import os
from concurrent.futures import ProcessPoolExecutor
from contextlib import AsyncExitStack
from functools import partial
from typing import Any, Awaitable, Callable, Dict, Optional, Union
from uuid import UUID

import anyio
import cloudpickle
from prefect.client.schemas import State
from prefect.futures import PrefectFuture
from prefect.states import exception_to_crashed_state
from prefect.task_runners import BaseTaskRunner, R, TaskConcurrencyType
from prefect.utilities.annotations import BaseAnnotation

from .utilities.collections import visit_collection_async
from .utilities.remote import run_remote_task


class MultiprocessTaskRunner(BaseTaskRunner):
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

    def __init__(self, processes: Union[int, None] = None):
        self.processes: Union[int, None] = processes
        self._futures: Dict[UUID, asyncio.Future] = {}
        super().__init__()

    @property
    def concurrency_type(self) -> TaskConcurrencyType:
        return TaskConcurrencyType.PARALLEL

    def duplicate(self) -> "MultiprocessTaskRunner":
        """
        Returns a new instance of `MultiprocessTaskRunner` using the same settings
        as this instance.
        """
        return MultiprocessTaskRunner(processes=self.processes)

    async def submit(
        self,
        key: UUID,
        call: Callable[..., Awaitable[State[R]]],
    ) -> None:
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

        # Prefect passes the call as a partial, so it's
        # easy to extract the kwargs. Here, we check if the callable
        # is a partial, and if so, we extract the kwargs and await
        # any PrefectFutures in the kwargs.
        if isinstance(call, partial):
            call_function = call.func
            call_kwargs = await self._resolve_prefect_futures(call.keywords)

        # If the call is somehow not a partial, raise
        # an error because we don't know how to unpack the
        # arguments to resolve any PrefectFutures.
        else:
            raise TypeError(
                f"MultiprocessTaskRunner doesn't know how to handle "
                f"calls of type {type(call)}."
            )

        # The default pickler can't handle Prefect tasks,
        # so we pre-pickle the function before shipping it.
        pickled_task = cloudpickle.dumps(partial(call_function, **call_kwargs))

        self._futures[key] = asyncio.wrap_future(
            self._executor.submit(run_remote_task, pickled_task)
        )

    async def wait(
        self, key: UUID, timeout: Union[float, None] = None
    ) -> Optional[State]:
        """
        Waits for the completion of a task with a given key and then returns
        its state.
        """

        result: Union[State, None] = None

        with anyio.move_on_after(timeout):
            try:
                await self._futures[key]
                result = cloudpickle.loads(self._futures[key].result())
                del self._futures[key]
            except BaseException as exc:
                result = await exception_to_crashed_state(exc)

        return result

    async def _resolve_prefect_futures(self, expr: Any):
        """
        Resolves any `PrefectFuture`s in task arguments
        before proceeding.
        """

        async def visit(expr):
            """
            Resolves Prefect futures when used as dependencies
            """
            # unwrap annotations like `allow_failure`
            if isinstance(expr, BaseAnnotation):
                expr = expr.unwrap()
            if isinstance(expr, PrefectFuture):
                running_task = self._futures[expr.key]
                if running_task:
                    await running_task
                    return cloudpickle.loads(running_task.result())

            return expr

        return await visit_collection_async(expr, visit_fn=visit, return_data=True)

    async def _start(self, exit_stack: AsyncExitStack):
        """
        Start the task runner and prep for context exit.
        """
        # Use `spawn` to create processes, because forking breaks Prefect.
        ctx = mp.get_context("spawn")

        process_count = self.processes if self.processes is not None else os.cpu_count()
        self._executor = ProcessPoolExecutor(max_workers=process_count, mp_context=ctx)

        exit_stack.push(self)

        self.logger.info(
            (
                "Created a multiprocessing task runner with "
                f"{process_count} processes."
            )
        )

    def __eq__(self, other: "MultiprocessTaskRunner") -> bool:
        """
        Equality comparison to determine if two `MultiprocessTaskRunner`s are the same.
        """
        if type(self) != type(other):
            return False

        return self.processes == other.processes

    def __exit__(self, *args):
        """
        Cleans up shuts down the task runner.
        """
        self._executor.shutdown(wait=True)

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
