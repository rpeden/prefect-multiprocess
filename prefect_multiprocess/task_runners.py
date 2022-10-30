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
from datetime import timedelta
from typing import Any, Awaitable, Callable, Dict, Optional, TypeVar
from uuid import UUID


import anyio
import anyio.to_process
import cloudpickle
import pickle
import prefect

import dill
import os
import pendulum
from multiprocessing.pool import Pool
from multiprocess.pool import AsyncResult
from multiprocess.managers import SharedMemoryManager
import multiprocessing
from prefect.utilities.collections import visit_collection_async
from prefect.futures import PrefectFuture
from prefect.orion.schemas.states import State
from prefect.states import exception_to_crashed_state
from prefect.task_runners import BaseTaskRunner, R, TaskConcurrencyType
from prefect.engine import begin_task_run

from prefect.utilities.asyncutils import (
    run_sync_in_worker_thread,
    sync_compatible,
    run_async_from_worker_thread,
    run_async_in_new_loop,
    sync,
)

# from prefect.utilities.asyncutils import sync_compatible
# from prefect.utilities.collections import visit_collection
from pendulum.duration import Duration
from pendulum.datetime import DateTime
from pendulum.time import Time
import json
import typing
from unittest.mock import Mock
from typing_extensions import TypeVarTuple, ParamSpec, ParamSpecArgs, ParamSpecKwargs
from functools import partial
import asyncio
import time
import threading
from multiprocessing import Manager

import prefect
from anyio.lowlevel import current_token

async def wait(func, *args):
    result = await func(*args)
    return await result


def cb(ev):
    ev.set()


class WrappedFunc:
    def __init__(self, func, task_run_proxy):
        self.func = func
        self.task_run_proxy = task_run_proxy

    async def __call__(self, pickled_call):
        #    print(f"trying to run this damn thing in {os.getpid()}")
        #    proxy_list = self.task_run_proxy
        #    task_run = proxy_list[0]
        #    kwargs["task_run"] = task_run#self.task_run_proxy.get_task_run()
        # new_args = tuple(args_list.items())
        unpickled_call = cloudpickle.loads(self.task_run_proxy.task_run)
        try:
            unpickled_call.keywords[
                "settings"
            ] = prefect.settings.get_default_settings()
            res = await unpickled_call()  # ()  # begin_task_run(**kwargs)
            # g = asyncio.gather(res)
            # g.add_done_callback(partial(cb, self.task_run_proxy.event))
            # fut = asyncio.gather(res)
        except Exception as e:
            print(f"wtf {e}")
        # res = self.func(*args)
        # self.ev.set()
        return res  # await self.func(*args)
        # future = asyncio.gather(self.func(*args))
        # tg = anyio.create_task_group()
        # coro = tg.start(self.func(*args))
        # result = anyio.to_thread.run_sync(wait, self.func, *args)
        # result = asyncio.gather(result)
        # while not result.done():
        #    print("waiting for result...")
        #    time.sleep(1)
        # return result.result()


class ThreadReturn():
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
        #os.environ["PREFECT_ORION_DATABASE_CONNECTION_URL"] = "sqlite+aiosqlite:///file::memory:?cache=shared&uri=true&check_same_thread=false"
        #unpickled_call.keywords["settings"] =  prefect.settings.get_settings_from_env()#prefect.settings.get_default_settings()
        result = ThreadReturn()
        worker_thread = threading.Thread(target=run_task, args=(unpickled_call, result))
        worker_thread.start()
        worker_thread.join()
        res = cloudpickle.dumps(result.result)#anyio.run(unpickled_call)  # begin_task_run(**kwargs)
        return (res, key)
        #queue.put((res, key))
        # fut = asyncio.gather(res)
    except Exception as e:
        print(f"wtf {e}")
    # unpickled_call = cloudpickle.loads(func)
    # result = unpickled_call()
    # return result


def unpickle_json(pickled) -> Any:
    return json.loads(pickled)


def pickle_json(pickler, obj) -> Any:
    pickled = json.dumps(obj)
    pickler.save_reduce(unpickle_json, (pickled,), obj=obj)


def unpickle_cloud(pickled) -> Any:
    return cloudpickle.loads(pickled)


abc = ParamSpec("P")


def unpickle_ohshit(pickled) -> Any:
    # print(f"trying to unpickle {type(obj)}")
    return cloudpickle.loads(pickled)


def pickle_ohshit(pickler, obj: Any):
    print(f"trying to pickle {type(obj)}")
    pickled = cloudpickle.dumps(Mock())
    pickler.save_reduce(unpickle_ohshit, (pickled,), obj=obj)


def pickle_cloud(pickler, obj: Any):
    pickled = cloudpickle.dumps(obj)
    pickler.save_reduce(unpickle_cloud, (pickled,), obj=obj)


def unpickle_coroutine(pickled) -> Any:
    # print(f"trying to unpickle {type(obj)}")
    return cloudpickle.loads(pickled)


def pickle_coroutine(pickler, obj: Any):
    print(f"trying to pickle {type(obj)}")
    result = obj
    pickler.save_reduce(unpickle_coroutine, (pickled,), obj=result)


def unpickle_function(pickled) -> Any:
    # print(f"trying to unpickle {type(obj)}")
    unpickled = cloudpickle.loads(pickled)
    return unpickled


def pickle_function(pickler, obj: Any):
    pickled = cloudpickle.dumps(obj)
    pickler.save_reduce(unpickle_function, (pickled,), obj=obj)


for t in (
    Duration,
    DateTime,
    Time,
):
    dill.register(t)(pickle_cloud)

for t in (
    TypeVarTuple,
    ParamSpec,
    ParamSpecArgs,
    ParamSpecKwargs,
):  # , type):
    dill.register(t)(pickle_ohshit)


async def hmm():
    pass


dill.register(WrappedFunc)(pickle_function)


def wrapper(func):
    async def wrapped(*args):
        result = await func(*args)
        return result

    return wrapped


class MyManager(SharedMemoryManager):
    pass


class TaskRunProxy:
    _exposed_ = ("get_task_run", "task_run", "args", "event")

    def __init__(self, task_run, event, args):
        self.task_run = task_run
        self.args = args
        self.event = event

    def get_task_run(self):
        return self.task_run

    # @property
    # def task_run(self):
    #    return self._task_run

class Event():
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
        #self.queue = multiprocessing.Queue()
        # self.manager = Manager()
        # self.manager.start()
        # self.mgr_events = self.manager.dict()
        # multiprocessing.set_start_method('spawn')

        super().__init__()

    @property
    def concurrency_type(self) -> TaskConcurrencyType:
        return TaskConcurrencyType.PARALLEL

    # def check_queue(self):
    #     while True:
    #         if self.queue.empty():
    #             time.sleep(0)
    #         else:
    #             res = self.queue.get()
    #             (result, key) = res
    #             self._task_results[key] = cloudpickle.loads(result)
    #             self._task_completion_events[key].set()

        
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
        
        unwrapped_function = call.func  # noqa
        task = call.keywords["task"]  # noqa
        #loop = current_token()
        # result = self._pool.apply_async(call, ())
        #with anyio.from_thread.start_blocking_portal() as p:
        #event = p.call(anyio.Event)
        self._task_completion_events[key] = Event()

        self.logger.info(f"Added task {key}, now {len(self._task_completion_events)} waiting completion.")
        
        def callback(ret):
            (result, key) = ret
            from anyio.from_thread import start_blocking_portal
            self.logger.info(f"in thread {threading.get_ident()}")
            self.logger.info(f"callback triggered with {len(self._task_completion_events)} in dict.")
            self._task_results[key] = cloudpickle.loads(result)
            #flow_run_context = prefect.context.FlowRunContext.get()
            #flow_run_context.call()
            #with anyio.from_thread.start_blocking_portal() as p:
            #    p.call(self._task_completion_events[key].set)
            self._task_completion_events[key].set()

        #ev = self._manager.Event()
        #self._task_completion_events[key] = ev
        # self.mgr_events[key] = ev
        # proxy_list = self._manager.list()
        # run_proxy = self._manager.TaskRunProxy(call.keywords["task_run"], ev, call.keywords)
        # proxy_list.append(call.keywords["task_run"])
        # del call.keywords["settings"]
        call_kwargs = await self._optimize_futures(call.keywords)
        print(f"pid is {os.getpid()}")
        #pickled_call = cloudpickle.dumps(call)
        try:
            pickled_func = cloudpickle.dumps(partial(call.func, **call_kwargs))
        except Exception as e:
            self.logger.info(e)
            raise e
        # tp = TaskRunProxy(None, ev, pickled_call)
        # print(f"pickled call: {pickled_call}")
        #tp = TaskRunProxy(pickled_call, ev, call.keywords)
        #wrapped_func = WrappedFunc(call, tp)
        #result = await run_sync_in_worker_thread(
        #    self._pool.apply_async,
        #    partial(wrapped_func, pickled_call),
        #    tuple(),
        #    callback=callback,
        #)
        # )
        # return result
        # self._pool.apply_async(
        #    partial(wrapped_func, call.keywords), tuple(), callback=callback
        # )
        self._pool.apply_async(
            partial(run, pickled_func), (key,), callback=callback
        )
        #await run_sync_in_worker_thread(self._pool.apply_async, partial(run, pickled_call), (key,), callback=callback)

    async def wait(self, key: UUID, timeout: float = None) -> Optional[State]:
        print(f"calling wait for {str(key)}")

        result = None

        with anyio.move_on_after(timeout):
            try:
                print(f"calling result get for task {str(key)}")
                await self._task_completion_events[key].wait()
                #del self._task_completion_events[key]
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
        self._manager = Manager()
        # self._manager.register("Task", prefect.Task)
        #self._manager.register("TaskRunProxy", TaskRunProxy)
        #elf._manager.start()
        #self.queue = self._manager.Queue()
        # self._manager.start()
        #exit_stack.push(self.queue)
        #threading.Thread(target=self.check_queue).start()
        exit_stack.push(self._pool)
        #exit_stack.push(self._manager)

        # exit_stack.push(self._manager)

        self.logger.info(
            (
                "Created a multiprocessing pool with "
                f"{self.number_of_processes} processes."
            )
        )

    def _cleanup(self):
        self.logger.info("shutting down processes...")
        #.shutdown()
        self._pool.close()

        # exit_stack.push(self._pool)
