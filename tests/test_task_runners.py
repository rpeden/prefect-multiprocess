import asyncio
from functools import partial
from unittest.mock import Mock
from uuid import uuid4

import pytest
from prefect import flow, task, State
from prefect.client.schemas import TaskRun
from prefect.results import UnpersistedResult
from prefect.states import State, StateType
from prefect.testing.standard_test_suites import TaskRunnerStandardTestSuite

from prefect_multiprocess import MultiprocessTaskRunner


class TestMultiprocessTaskRunner(TaskRunnerStandardTestSuite):
    @pytest.fixture(scope="session")
    def task_runner(self):
        yield MultiprocessTaskRunner(processes=2)

    @pytest.mark.parametrize("exception", [KeyboardInterrupt(), ValueError("test")])
    async def test_wait_captures_exceptions_as_crashed_state(
        self, task_runner, exception
    ):
        async def fake_begin_task_run(task, task_run):
            raise exception

        test_key = uuid4()

        async with task_runner.start():
            await task_runner.submit(
                call=partial(fake_begin_task_run, task=Mock(), task_run=Mock()),
                key=test_key,
            )

            state = await task_runner.wait(test_key, 15)
            assert state is not None, "wait timed out"
            assert isinstance(state, State), "wait should return a state"
            assert state.name == "Crashed"

    # updated to ensure correct result is awaited with fetch=True
    async def test_submit_and_wait(self, task_runner):
        task_run = TaskRun(flow_run_id=uuid4(), task_key="foo", dynamic_key="bar")

        async def fake_orchestrate_task_run(example_kwarg):
            return State(
                type=StateType.COMPLETED,
                data=await UnpersistedResult.create(example_kwarg),
            )

        async with task_runner.start():
            await task_runner.submit(
                key=task_run.id,
                call=partial(fake_orchestrate_task_run, example_kwarg=1),
            )
            state = await task_runner.wait(task_run.id, 30)
            assert state is not None, "wait timed out"
            assert isinstance(state, State), "wait should return a state"
            assert await state.result(fetch=True) == 1

    async def test_async_task_timeout(self, task_runner):
        @task(timeout_seconds=0.1)
        async def my_timeout_task():
            await asyncio.sleep(2)
            return 42

        @task
        async def my_dependent_task(task_res):
            return 1764

        @task
        async def my_independent_task():
            return 74088

        @flow(version="test", task_runner=task_runner)
        async def test_flow():
            a = await my_timeout_task.submit()
            b = await my_dependent_task.submit(a)
            c = await my_independent_task.submit()

            return a, b, c

        state: State = await test_flow._run()

        assert state.is_failed()
        ax, bx, cx = await state.result(raise_on_failure=False, fetch=True)
        assert ax.type == StateType.FAILED
        assert bx.type == StateType.PENDING
        assert cx.type == StateType.COMPLETED
