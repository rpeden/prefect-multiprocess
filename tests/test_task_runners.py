from functools import partial
from unittest.mock import Mock
from uuid import uuid4
from prefect import State
import pytest
from prefect.testing.standard_test_suites import TaskRunnerStandardTestSuite
from prefect_multiprocess import MultiprocessTaskRunner


class TestMultiprocessTaskRunner(TaskRunnerStandardTestSuite):
    @pytest.fixture
    def task_runner(self):
        yield MultiprocessTaskRunner()

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
