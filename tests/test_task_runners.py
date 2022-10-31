from functools import partial
from unittest.mock import Mock
from uuid import uuid4
from prefect import BaseResult, State
import pytest
from prefect.client.schemas import TaskRun
from prefect.deprecated.data_documents import DataDocument
from prefect.orion.schemas.states import StateType
from prefect.testing.standard_test_suites import TaskRunnerStandardTestSuite
from prefect_multiprocess import MultiprocessTaskRunner

task_runner = MultiprocessTaskRunner(number_of_processes=2)

class TestMultiprocessTaskRunner(TaskRunnerStandardTestSuite):
    @pytest.fixture
    def task_runner(self):
        yield task_runner

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

            state = await task_runner.wait(test_key, 5)
            assert state is not None, "wait timed out"
            assert isinstance(state, State), "wait should return a state"
            assert state.name == "Crashed"

    # updated to ensure correct result is awaited with fetch=True
    async def test_submit_and_wait(self, task_runner):
        task_run = TaskRun(flow_run_id=uuid4(), task_key="foo", dynamic_key="bar")

        async def fake_orchestrate_task_run(example_kwarg):
            return State(
                type=StateType.COMPLETED,
                data=DataDocument.encode("json", example_kwarg),
            )

        async with task_runner.start():
            await task_runner.submit(
                key=task_run.id,
                call=partial(fake_orchestrate_task_run, example_kwarg=1),
            )
            state = await task_runner.wait(task_run.id, 10)
            assert state is not None, "wait timed out"
            assert isinstance(state, State), "wait should return a state"
            assert await state.result(fetch=True) == 1
