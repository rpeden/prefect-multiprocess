"""
Interface and implementations of the Ray Task Runner.
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

    Switching to a `RayTaskRunner`:
    ```python
    import time

    from prefect import flow, task
    from prefect_ray import RayTaskRunner

    @task
    def shout(number):
        time.sleep(0.5)
        print(f"#{number}")

    @flow(task_runner=RayTaskRunner)
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
from typing import Awaitable, Callable, Optional, Union
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


def run_task(task_fn, result_obj):
    result = anyio.run(run_task_async, task_fn)
    result_obj.set_result(result)


import os


def run(func, key):
    unpickled_call = cloudpickle.loads(func)
    print(f"task running in pid {os.getpid()}")
    try:
        # run in a new thread so we can run the task
        # in async context
        result = ThreadReturn()
        worker_thread = threading.Thread(target=run_task, args=(unpickled_call, result))
        worker_thread.start()
        worker_thread.join()

        # result = anyio.run(run_task_async, unpickled_call)
        res = cloudpickle.dumps(
            result.return_value
        )  # anyio.run(unpickled_call)  # begin_task_run(**kwargs)
        return (res, key)
        # queue.put((res, key))
        # fut = asyncio.gather(res)
    except Exception as e:
        print(f"wtf {e}")
    # unpickled_call = cloudpickle.loads(func)
    # result = unpickled_call()
    # return result


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
    A parallel task_runner that submits tasks to `ray`.
    By default, a temporary Ray cluster is created for the duration of the flow run.
    Alternatively, if you already have a `ray` instance running, you can provide
    the connection URL via the `address` kwarg.
    Args:
        address (string, optional): Address of a currently running `ray` instance; if
            one is not provided, a temporary instance will be created.
        init_kwargs (dict, optional): Additional kwargs to use when calling `ray.init`.
    Examples:
        Using a temporary local ray cluster:
        ```python
        from prefect import flow
        from prefect_ray.task_runners import RayTaskRunner

        @flow(task_runner=RayTaskRunner())
        def my_flow():
            ...
        ```
        Connecting to an existing ray instance:
        ```python
        RayTaskRunner(address="ray://192.0.2.255:8786")
        ```
    """

    def __init__(self, number_of_processes: Union[int, None] = None):
        self.number_of_processes = number_of_processes
        self._task_results = {}
        self._task_completion_events = {}
        self.completed = 0

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

        self._task_completion_events[key] = Event()

        def callback(ret):
            (result, key) = ret
            self._task_results[key] = cloudpickle.loads(result)
            self._task_completion_events[key].set()

        call_kwargs = await self._optimize_futures(call.keywords)

        try:
            pickled_func = cloudpickle.dumps(partial(call.func, **call_kwargs))
        except Exception as e:
            self.logger.info(e)
            raise e

        self._pool.apply_async(
            partial(run, pickled_func),
            (key,),
            callback=callback,
        )

    async def wait(self, key: UUID, timeout: float = None) -> Optional[State]:
        print(f"calling wait for {str(key)}")

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
        Exchange PrefectFutures for ray-compatible futures
        """

        async def visit_fn(expr):
            """
            Resolves ray futures when used as dependencies
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

        - Creates a cluster if an external address is not set.
        - Creates a client to connect to the cluster.
        - Pushes a call to wait for all running futures to complete on exit.
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
