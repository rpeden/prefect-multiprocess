import anyio
import cloudpickle

from functools import partial
from prefect.task_engine import run_task_sync, run_task_async

import prefect


def run_remote_task(pickled_task: bytes, pickled_context: bytes) -> bytes:
    """
    Runs a task that has been serialized with cloudpickle.
    This function is intended to be used in a remote context, such as a worker process.
    """
    task: prefect.Task | None = None
    submit_kwargs: dict = {}
    [task, submit_kwargs] = cloudpickle.loads(pickled_task)
    context = cloudpickle.loads(pickled_context)

    task_result = None
    if task and task.isasync:
        task_result = anyio.run(
            partial(run_task_async, task=task, context=context, **submit_kwargs)
        )
    else:
        task_run = partial(run_task_sync, task=task, context=context, **submit_kwargs)
        task_result = task_run()

    return cloudpickle.dumps(task_result)
