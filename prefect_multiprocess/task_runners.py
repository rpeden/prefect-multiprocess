"""
Interface and implementations of the Multiprocess task runner.
[Task Runners](https://orion-docs.prefect.io/api-ref/prefect/task-runners/)
in Prefect are responsible for managing the execution of Prefect task runs.
Generally speaking, users are not expected to interact with
task runners outside of configuring and initializing them for a flow.

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

    @flow(task_runner=MultiprocessTaskRunner)
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

from contextlib import AsyncExitStack
from functools import partial
from multiprocessing.pool import Pool
import threading
from typing import Awaitable, Callable, Optional, Tuple, Union
from uuid import UUID

import anyio
import cloudpickle
from anyio.lowlevel import current_token
from prefect.futures import PrefectFuture
from prefect.orion.schemas.states import State
from prefect.states import exception_to_crashed_state
from prefect.task_runners import BaseTaskRunner, R, TaskConcurrencyType

from .utilities.collections import visit_collection_async


class ThreadReturn:
    def set_result(self, return_value):
        self.return_value = return_value


async def run_task_async(task_fn):
    try:
        result = await task_fn()
        return result
    except BaseException as exc:
        return await exception_to_crashed_state(exc)


def task_thread(task_fn, result_obj):
    result = anyio.run(run_task_async, task_fn)
    result_obj.set_result(result)


def run_remote_task(task_callable, key):
    unpickled_call = cloudpickle.loads(task_callable)
    # run the remote task in a new thread
    task_output = ThreadReturn()
    worker_thread = threading.Thread(
        target=task_thread, args=(unpickled_call, task_output)
    )
    worker_thread.start()
    worker_thread.join()

    result = cloudpickle.dumps(
        task_output.return_value
    )  
    return (result, key)



class Event:
    def __init__(self):
        self._loop = current_token()
        self._event = anyio.Event()

    def set(self):
        self._loop.call_soon_threadsafe(self._event.set)

    async def wait(self):
        await self._event.wait()


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

    def __init__(self, number_of_processes: Union[int, None] = None):
        self.number_of_processes = number_of_processes
        self._task_results = {}
        self._task_completion_events = {}

        super().__init__()

    @property
    def concurrency_type(self) -> TaskConcurrencyType:
        return TaskConcurrencyType.PARALLEL

    async def submit(
        self,
        key: UUID,
        call: Callable[..., Awaitable[State[R]]],
    ) -> None:
        if not self._started:
            raise RuntimeError(
                "The task runner must be started before submitting work."
            )
        """
        Submits a task to the task runner.
        """
        def remote_task_callback(task_output: Tuple[bytes,UUID]):
            (result, key) = task_output
            self._task_results[key] = cloudpickle.loads(result)
            self._task_completion_events[key].set()

        self._task_completion_events[key] = Event()
        call_kwargs = await self._optimize_futures(call.keywords)

        pickled_task = cloudpickle.dumps(partial(call.func, **call_kwargs))

        self._pool.apply_async(
            partial(run_remote_task, pickled_task),
            (key,),
            callback=remote_task_callback,
        )

    async def wait(self, key: UUID, timeout: float = None) -> Optional[State]:
        """
        Waits for the completion of a task with a given key and then returns
        its state.
        """

        result: State = None

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

        self.logger.info("Creating a local multiprocessing pool...")
        self._pool = Pool(processes=self.number_of_processes)
        exit_stack.push(self._pool)

        self.logger.info(
            (
                "Created a multiprocessing pool with "
                f"{self.number_of_processes} processes."
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
