"""This is an example flows module"""
from prefect import flow

from prefect_multiprocess.blocks import MultiprocessBlock
from prefect_multiprocess.tasks import (
    goodbye_prefect_multiprocess,
    hello_prefect_multiprocess,
)


@flow
def hello_and_goodbye():
    """
    Sample flow that says hello and goodbye!
    """
    MultiprocessBlock.seed_value_for_example()
    block = MultiprocessBlock.load("sample-block")

    print(hello_prefect_multiprocess())
    print(f"The block's value: {block.value}")
    print(goodbye_prefect_multiprocess())
    return "Done"


if __name__ == "__main__":
    hello_and_goodbye()
