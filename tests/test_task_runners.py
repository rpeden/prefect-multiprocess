from concurrent.futures import Future
from typing import Any, Iterable, Optional
import time
import uuid

import pytest

from prefect.context import TagsContext, tags
from prefect.filesystems import LocalFileSystem
from prefect.flows import flow
from prefect.futures import PrefectFuture, PrefectWrappedFuture
from prefect.settings import (
    PREFECT_DEFAULT_RESULT_STORAGE_BLOCK,
    PREFECT_TASK_SCHEDULING_DEFAULT_STORAGE_BLOCK,
    temporary_settings,
)
from prefect.states import Completed, Running
from prefect.tasks import task

from prefect_multiprocess import MultiprocessTaskRunner
from prefect_multiprocess.task_runners import MultiprocessPrefectFuture


@task
def my_test_task(param1, param2):
    return param1, param2


@task
async def my_test_async_task(param1, param2):
    return param1, param2


@task
def context_matters(param1=None, param2=None):
    return TagsContext.get().current_tags


@task
async def context_matters_async(param1=None, param2=None):
    return TagsContext.get().current_tags


class MockFuture(PrefectWrappedFuture):
    def __init__(self, data: Any = 42):
        super().__init__(uuid.uuid4(), Future())
        self._data = data
        self._state = Running()

    def wait(self, timeout: Optional[float] = None) -> None:
        self._state = Completed(data=self._data)

    def result(
        self,
        timeout: Optional[float] = None,
        raise_on_failure: bool = True,
    ) -> Any:
        self.wait()
        return self._state.result()

    @property
    def state(self) -> Any:
        return self._state


class TestMultiprocessTaskRunner:
    @pytest.fixture(autouse=True)
    def default_storage_setting(self, tmp_path):
        name = str(uuid.uuid4())
        LocalFileSystem(basepath=tmp_path).save(name)
        with temporary_settings(
            {
                PREFECT_DEFAULT_RESULT_STORAGE_BLOCK: f"local-file-system/{name}",
                PREFECT_TASK_SCHEDULING_DEFAULT_STORAGE_BLOCK: f"local-file-system/{name}",
            }
        ):
            yield

    def test_duplicate(self):
        runner = MultiprocessTaskRunner()
        duplicate_runner = runner.duplicate()
        assert isinstance(duplicate_runner, MultiprocessTaskRunner)
        assert duplicate_runner is not runner
        assert duplicate_runner == runner

    def test_runner_must_be_started(self):
        runner = MultiprocessTaskRunner()
        with pytest.raises(
            RuntimeError,
            match="The task runner must be started before submitting work.",
        ):
            runner.submit(my_test_task, {})

    def test_set_max_workers(self):
        with MultiprocessTaskRunner(processes=2) as runner:
            assert runner._executor._max_workers == 2  # type: ignore

    def test_submit_sync_task(self):
        with MultiprocessTaskRunner() as runner:
            parameters = {"param1": 1, "param2": 2}
            future = runner.submit(my_test_task, parameters)
            assert isinstance(future, PrefectFuture)
            assert isinstance(future.task_run_id, uuid.UUID)
            assert isinstance(future.wrapped_future, Future)

            assert future.result() == (1, 2)

    def test_submit_async_task(self):
        with MultiprocessTaskRunner() as runner:
            parameters = {"param1": 1, "param2": 2}
            future = runner.submit(my_test_async_task, parameters)
            assert isinstance(future, PrefectFuture)
            assert isinstance(future.task_run_id, uuid.UUID)
            assert isinstance(future.wrapped_future, Future)

            assert future.result() == (1, 2)

    def test_submit_sync_task_receives_context(self):
        with tags("tag1", "tag2"):
            with MultiprocessTaskRunner() as runner:
                future = runner.submit(context_matters, {})
                assert isinstance(future, PrefectFuture)
                assert isinstance(future.task_run_id, uuid.UUID)
                assert isinstance(future.wrapped_future, Future)

                assert future.result() == {"tag1", "tag2"}

    def test_submit_async_task_receives_context(self):
        with tags("tag1", "tag2"):
            with MultiprocessTaskRunner() as runner:
                future = runner.submit(context_matters_async, {})
                assert isinstance(future, PrefectFuture)
                assert isinstance(future.task_run_id, uuid.UUID)
                assert isinstance(future.wrapped_future, Future)

                assert future.result() == {"tag1", "tag2"}

    def test_map_sync_task(self):
        with MultiprocessTaskRunner() as runner:
            parameters = {"param1": [1, 2, 3], "param2": [4, 5, 6]}
            futures = runner.map(my_test_task, parameters)
            assert isinstance(futures, Iterable)
            assert all(isinstance(future, PrefectFuture) for future in futures)
            assert all(isinstance(future.task_run_id, uuid.UUID) for future in futures)
            assert all(isinstance(future.wrapped_future, Future) for future in futures)  # type: ignore

            results = [future.result() for future in futures]
            assert results == [(1, 4), (2, 5), (3, 6)]

    def test_map_async_task(self):
        with MultiprocessTaskRunner() as runner:
            parameters = {"param1": [1, 2, 3], "param2": [4, 5, 6]}
            futures = runner.map(my_test_async_task, parameters)
            assert isinstance(futures, Iterable)
            assert all(isinstance(future, PrefectFuture) for future in futures)
            assert all(isinstance(future.task_run_id, uuid.UUID) for future in futures)
            assert all(isinstance(future.wrapped_future, Future) for future in futures)  # type: ignore

            results = [future.result() for future in futures]
            assert results == [(1, 4), (2, 5), (3, 6)]

    def test_map_sync_task_with_context(self):
        with tags("tag1", "tag2"):
            with MultiprocessTaskRunner() as runner:
                parameters = {"param1": [1, 2, 3], "param2": [4, 5, 6]}
                futures = runner.map(context_matters, parameters)
                assert isinstance(futures, Iterable)
                assert all(isinstance(future, PrefectFuture) for future in futures)
                assert all(
                    isinstance(future.task_run_id, uuid.UUID) for future in futures
                )
                assert all(
                    isinstance(future.wrapped_future, Future) # type: ignore
                    for future in futures  
                )

                results = [future.result() for future in futures]
                assert results == [{"tag1", "tag2"}] * 3

    def test_map_async_task_with_context(self):
        with tags("tag1", "tag2"):
            with MultiprocessTaskRunner() as runner:
                parameters = {"param1": [1, 2, 3], "param2": [4, 5, 6]}
                futures = runner.map(context_matters_async, parameters)
                assert isinstance(futures, Iterable)
                assert all(isinstance(future, PrefectFuture) for future in futures)
                assert all(
                    isinstance(future.task_run_id, uuid.UUID) for future in futures
                )
                assert all(
                    isinstance(future.wrapped_future, Future) # type: ignore
                    for future in futures  
                )

                results = [future.result() for future in futures]
                assert results == [{"tag1", "tag2"}] * 3

    def test_map_with_future_resolved_to_list(self):
        with MultiprocessTaskRunner() as runner:
            future = MockFuture(data=[1, 2, 3])
            parameters = {"param1": future, "param2": future}
            futures = runner.map(my_test_task, parameters)
            assert isinstance(futures, Iterable)
            assert all(isinstance(future, PrefectFuture) for future in futures)
            assert all(isinstance(future.task_run_id, uuid.UUID) for future in futures)
            assert all(isinstance(future.wrapped_future, Future) for future in futures)  # type: ignore

            results = [future.result() for future in futures]
            assert results == [(1, 1), (2, 2), (3, 3)]

    def test_handles_recursively_submitted_tasks(self):
        """
        Regression test for https://github.com/PrefectHQ/prefect/issues/14194.

        This test ensures that the MultiprocessTaskRunner doesn't place an upper limit on the
        number of submitted tasks active at once. The highest default max workers on a
        ThreadPoolExecutor is 32, so this test submits 33 tasks recursively, which will
        deadlock without the MultiprocessTaskRunner setting the max_workers to sys.maxsize.
        """

        @task
        def recursive_task(n):
            if n == 0:
                return n
            time.sleep(0.1)
            future = recursive_task.submit(n - 1)  # type: ignore
            return future.result()

        @flow
        def test_flow():
            return recursive_task.submit(33)

        assert test_flow().result() == 0
