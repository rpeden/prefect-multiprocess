import threading

import anyio
import cloudpickle
from prefect.states import exception_to_crashed_state


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

    result = cloudpickle.dumps(task_output.return_value)

    return (result, key)
