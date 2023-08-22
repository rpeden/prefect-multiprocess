import anyio
import cloudpickle
from prefect.states import exception_to_crashed_state, exception_to_failed_state


async def run_task_async(task_fn, timeout_seconds=None):
    try:
        with anyio.move_on_after(timeout_seconds) as scope:
            result = await task_fn()

        if scope.cancel_called:
            exc = TimeoutError(f"Task timed out after {timeout_seconds} seconds.")
            result = await exception_to_failed_state(exc)

        return result
    except BaseException as exc:
        return await exception_to_crashed_state(exc)


def run_remote_task(task_callable):
    unpickled_call = cloudpickle.loads(task_callable)
    task = unpickled_call.keywords.get("task")
    timeout_seconds = task.timeout_seconds if task else None

    task_result = anyio.run(run_task_async, unpickled_call, timeout_seconds)
    return cloudpickle.dumps(task_result)
