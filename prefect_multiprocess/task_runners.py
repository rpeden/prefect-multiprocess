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
import multiprocessing as mp
import os
from contextlib import AsyncExitStack
from functools import partial
from multiprocessing.pool import Pool
from typing import Awaitable, Callable, Dict, Optional, Tuple, Union
from uuid import UUID

import anyio
import cloudpickle
from prefect.client.schemas import State
from prefect.futures import PrefectFuture
from prefect.states import exception_to_crashed_state
from prefect.task_runners import BaseTaskRunner, R, TaskConcurrencyType
from prefect.utilities.annotations import BaseAnnotation

from .utilities.asyncutils import Event
from .utilities.collections import visit_collection_async
from .utilities.remote import run_remote_task


class MultiprocessTaskRunner(BaseTaskRunner):
    """
    A parallel task_runner that submits tasks to uses Python's `multiprocessing`
    module to run tasks using multiple Python processes.
    Args:
        number_of_processes (int, optional): The number of processes to use for
        running tasks. If not provided, defaults to the number of CPUs on the
        host machine.
    Examples:

        Create one process for every CPU/CPU core
        ```python
        from prefect import flow
        from prefect_multiprocess.task_runners import MultiprocessTaskRunner


        @flow(task_runner=MultiprocessTaskRunner())
        def my_flow():
            ...
        ```

        Customizing the number of processes:
        ```python
        @flow(task_runner=MultiprocessTaskRunner(number_of_processes=4))
        def my_flow():
            ...
        ```
    """

    def __init__(self, processes: Union[int, None] = None):
        self.processes = processes
        self._task_results = {}
        self._task_completion_events = {}

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

        if self._pool is None:
            raise RuntimeError(
                "Can't run tasks in a MultiprocessingTaskRunner after serialization."
            )

        def remote_task_callback(task_output: Tuple[bytes, UUID]):
            """
            Callback for when a remote task completes. This is called
            in the main process.
            """
            (result, task_key) = task_output
            self._task_results[task_key] = cloudpickle.loads(result)
            self._task_completion_events[task_key].set()

        self._task_completion_events[key] = Event()

        # Prefect currently passes the call as a partial, so it's
        # easy to extract the kwargs. Here, we check if the callable
        # is a partial, and if so, we extract the kwargs and await
        # any PrefectFutures in the kwargs.
        if isinstance(call, partial):
            call_function = call.func
            call_kwargs = await self._optimize_futures(call.keywords)
        # If the call is not a function or a partial, raise
        # an error because we don't know how to unpack the
        # arguments to resolve any PrefectFutures.
        else:
            raise TypeError(
                f"MultiprocessTaskRunner doesn't know how to handle "
                f"calls of type {type(call)}."
            )

        # `multiprocessing`'s default pickler can't handle Prefect tasks,
        # so we pre-pickle the function before shipping it to another
        # process
        pickled_task = cloudpickle.dumps(partial(call_function, **call_kwargs))

        self._pool.apply_async(
            partial(run_remote_task, pickled_task),
            (key,),
            callback=remote_task_callback,
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
                await self._task_completion_events[key].wait()
                result = self._task_results[key]
                del self._task_completion_events[key]
            except BaseException as exc:
                result = await exception_to_crashed_state(exc)

        return result

    async def _optimize_futures(self, expr):
        """
        Await PrefectFutures before proceeding.
        """

        async def visit_fn(expr):
            """
            Resolves Prefect futures when used as dependencies
            """
            if isinstance(expr, BaseAnnotation):
                expr = expr.unwrap()
            if isinstance(expr, PrefectFuture):
                running_task = self._task_completion_events[expr.key]
                if running_task:
                    await running_task.wait()
                    return self._task_results[expr.key]
            # Fallback to return the expression unaltered
            return expr

        return await visit_collection_async(expr, visit_fn=visit_fn, return_data=True)

    async def _start(self, exit_stack: AsyncExitStack):
        """
        Start the task runner and prep for context exit.
        """
        # Use `spawn` to create processes, because forking breaks Prefect.
        ctx = mp.get_context("spawn")

        self._pool = ctx.Pool(processes=self.processes)
        exit_stack.push(self._pool)

        process_count = self.processes if self.processes is not None else os.cpu_count()
        self.logger.info(
            (
                "Created a multiprocessing pool with "
                f"{process_count} processes."
            )
        )

    def __getstate__(self):
        """
        Allow the `MultiprocessTaskRunner` to be serialized by dropping the
        process pool.
        """
        data = self.__dict__.copy()
        data.update({k: None for k in {"_pool"}})
        return data

    def __setstate__(self, data: dict):
        """
        When deserialized, we will no longer have a reference to the process pool.
        """
        self.__dict__.update(data)
        self._pool = None
