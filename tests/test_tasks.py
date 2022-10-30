from prefect import flow

from prefect_multiprocess.tasks import (
    goodbye_prefect_multiprocess,
    hello_prefect_multiprocess,
)


def test_hello_prefect_multiprocess():
    @flow
    def test_flow():
        return hello_prefect_multiprocess()

    result = test_flow()
    assert result == "Hello, prefect-multiprocess!"


def goodbye_hello_prefect_multiprocess():
    @flow
    def test_flow():
        return goodbye_prefect_multiprocess()

    result = test_flow()
    assert result == "Goodbye, prefect-multiprocess!"
