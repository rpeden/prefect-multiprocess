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

import os
import threading
from concurrent.futures import ProcessPoolExecutor
from contextlib import AsyncExitStack
from functools import partial
from multiprocessing.pool import Pool
from typing import Awaitable, Callable, Dict, Optional
from uuid import UUID

import anyio
import anyio.to_process
import cloudpickle
from anyio.lowlevel import current_token
from prefect.futures import PrefectFuture
from prefect.orion.schemas.states import State
from prefect.states import exception_to_crashed_state
from prefect.task_runners import BaseTaskRunner, R, TaskConcurrencyType

from .utilities.collections import visit_collection_async


class ThreadReturn:
    def set_result(self, result):
        self.result = result


async def run_task_async(task_fn):
    result = await task_fn()
    return result


def run_task(task_fn, result_obj):
    result = anyio.run(run_task_async, task_fn)
    result_obj.set_result(result)


def run(func, key):
    print(f"running in process {os.getpid()}")
    unpickled_call = cloudpickle.loads(func)
    try:
        result = ThreadReturn()
        worker_thread = threading.Thread(target=run_task, args=(unpickled_call, result))
        worker_thread.start()
        worker_thread.join()
        res = cloudpickle.dumps(
            result.result
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

    def __init__(self, number_of_processes: int = 1):
        # Store settings
        self.number_of_processes = number_of_processes
        self._task_results = {}
        self._task_completion_events = {}
        self.completed = 0
        self.lock = threading.Lock()
        self.limiter = anyio.Semaphore(4)

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

        self.logger.info(f"calling submit for {str(key)}")
        self._task_completion_events[key] = Event()

        self.logger.info(
            f"Added task {key}, now {len(self._task_completion_events)} waiting completion."  # noqa
        )

        def callback(ret):
            (result, key) = ret

            self.logger.info(f"in thread {threading.get_ident()}")
            self.logger.info(
                f"callback triggered with {len(self._task_completion_events)} in dict."
            )
            self._task_results[key] = cloudpickle.loads(result)
            self._task_completion_events[key].set()

        call_kwargs = await self._optimize_futures(call.keywords)
        print(f"pid is {os.getpid()}")
        try:
            pickled_func = cloudpickle.dumps(partial(call.func, **call_kwargs))
        except Exception as e:
            self.logger.info(e)
            raise e
            # return result
            # self._pool.apply_async(
            #    partial(wrapped_func, call.keywords), tuple(), callback=callback
            # )
        self._pool.apply_async(
            partial(run, pickled_func),
            (key,),
            callback=callback,
        )
        # await anyio.sleep(0)

    async def wait(self, key: UUID, timeout: float = None) -> Optional[State]:
        print(f"calling wait for {str(key)}")

        result = None

        with anyio.move_on_after(timeout):
            try:
                print(f"calling result get for task {str(key)}")
                await self._task_completion_events[key].wait()
                # del self._task_completion_events[key]
                result = self._task_results[key]
            except Exception as e:
                print(f"got stupid exception for task {str(key)}: {str(e)}")
                result = await exception_to_crashed_state(e)
            except BaseException as exc:
                result = await exception_to_crashed_state(exc)

        self.logger.info(f"Returning result for task {key}")
        self.logger.info(f"{len(self._task_completion_events)} remain in queue")
        self.completed += 1
        self.logger.info(f"returned {self.completed} so far.")
        # we get a PrefectFuture result back, so we need to await it to return the state
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
        #  return await state.result()

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

    def _cleanup(self):
        self.logger.info("shutting down processes...")
        # .shutdown()
        self._pool.close()


class TaskResult:
    def __init__(self, result: Awaitable[State]):
        self._result = result

    async def wait(self) -> State:
        return await self._result


def run_task_in_worker(func):
    print(f"running in process {os.getpid()}")
    unpickled_call = cloudpickle.loads(func)
    try:
        result = ThreadReturn()
        worker_thread = threading.Thread(target=run_task, args=(unpickled_call, result))
        worker_thread.start()
        worker_thread.join()
        res = cloudpickle.dumps(
            result.result
        )  # anyio.run(unpickled_call)  # begin_task_run(**kwargs)
        return res
    except Exception as e:
        print(f"Broke in subprocess: {e.with_traceback}")
        raise e


class AnyioMultiprocessTaskRunner(BaseTaskRunner):
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

    def __init__(self, number_of_processes: int = 1):
        # Store settings
        self._number_of_processes = number_of_processes
        self._task_results: Dict[UUID, TaskResult] = {}
        self._task_completion_events = {}
        self.completed = 0
        self.lock = threading.Lock()
        self.limiter = anyio.Semaphore(4)

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

        self.logger.info(f"calling submit for {str(key)}")
        self._task_completion_events[key] = anyio.Event()

        self.logger.info(
            f"Added task {key}, now {len(self._task_completion_events)} waiting completion."  # noqa
        )

        call_kwargs = await self._optimize_futures(call.keywords)
        print(f"pid is {os.getpid()}")
        try:
            pickled_func = cloudpickle.dumps(partial(call.func, **call_kwargs))
        except Exception as e:
            self.logger.info(e)
            raise e

        task_run_call = partial(run_task_in_worker, pickled_func)

        # running_task = self._task_group.start(
        #     partial(anyio.to_process.run_sync, task_run_call, limiter=self._limiter)
        # )
        self._task_group.start_soon(
            self._run_and_store_result,
            key,
            partial(anyio.to_process.run_sync, task_run_call, limiter=self.limiter),
        )
        # await anyio.sleep(0)
        # self._task_results[key] = TaskResult(
        #     running_task
        #     # partial(anyio.to_process.run_sync, task_run_call, limiter=self._limiter)
        # )

        # self._task_results[key] = await anyio.to_process.run_sync(
        #     task_run_call, limiter=self._limiter
        # )
        # self._task_completion_events[key].set()

        # self._pool.apply_async(partial(run, pickled_func), (key,), callback=callback)

    async def _run_and_store_result(
        self, key: UUID, call: Callable[[], Awaitable[State[R]]]
    ):
        """
        Simple utility to store the orchestration result in memory on completion
        Since this run is occuring on the main thread, we capture exceptions to prevent
        task crashes from crashing the flow run.
        """
        try:
            self._task_results[key] = await call()
        except BaseException as exc:
            self._task_results[key] = await exception_to_crashed_state(exc)

        self._task_completion_events[key].set()

    async def wait(self, key: UUID, timeout: float = None) -> Optional[State]:
        print(f"calling wait for {str(key)}")

        result = None

        with anyio.move_on_after(timeout):
            try:
                print(f"anyio waiting for task: {str(key)}")
                # pickled_result = await self._task_results[key].wait()
                await self._task_completion_events[key].wait()
                pickled_result = self._task_results[key]
                # del self._task_completion_events[key]
                result = cloudpickle.loads(pickled_result)
            except Exception as e:
                print(f"got stupid exception for task {str(key)}: {str(e)}")
                result = await exception_to_crashed_state(e)
            except BaseException as exc:
                result = await exception_to_crashed_state(exc)

        self.logger.info(f"Returning result for task {key}")
        self.logger.info(f"{len(self._task_results)} remain in queue")
        self.completed += 1
        self.logger.info(f"returned {self.completed} so far.")
        # we get a PrefectFuture result back, so we need to await it to return the state
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
        #  return await state.result()

    async def _start(self, exit_stack: AsyncExitStack):
        """
        Start the task runner and prep for context exit.

        - Creates a cluster if an external address is not set.
        - Creates a client to connect to the cluster.
        - Pushes a call to wait for all running futures to complete on exit.
        """

        self.logger.info("Creating a local multiprocessing pool...")
        self._limiter = anyio.CapacityLimiter(self._number_of_processes)
        self._task_group = await exit_stack.enter_async_context(
            anyio.create_task_group()
        )
        # exit_stack.push(self._task_group)

        # self._pool = Pool(processes=self.number_of_processes)
        # exit_stack.push(self._pool)

        self.logger.info(
            (
                "Creating a multiprocessing pool with "
                f"{self._number_of_processes} processes."
            )
        )

    def _cleanup(self):
        self.logger.info("shutting down processes...")
        # .shutdown()
        self._pool.close()
