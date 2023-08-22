# Prefect Multiprocess Task Runner

<p align="center">
    <a href="https://pypi.python.org/pypi/prefect-multiprocess/" alt="PyPI version">
        <img alt="PyPI" src="https://img.shields.io/pypi/v/prefect-multiprocess?color=0052FF&labelColor=090422"></a>
    <a href="https://github.com/rpeden/prefect-multiprocess/" alt="Stars">
        <img src="https://img.shields.io/github/stars/rpeden/prefect-multiprocess?color=0052FF&labelColor=090422" /></a>
    <a href="https://pepy.tech/badge/prefect-multiprocess/" alt="Downloads">
        <img src="https://img.shields.io/pypi/dm/prefect-multiprocess?color=0052FF&labelColor=090422" /></a>
    <a href="https://github.com/rpeden/prefect-multiprocess/pulse" alt="Activity">
        <img src="https://img.shields.io/github/commit-activity/m/rpeden/prefect-multiprocess?color=0052FF&labelColor=090422" /></a>
    <br>
    <a href="https://prefect-community.slack.com" alt="Slack">
        <img src="https://img.shields.io/badge/slack-join_community-red.svg?color=0052FF&labelColor=090422&logo=slack" /></a>
    <a href="https://discourse.prefect.io/" alt="Discourse">
        <img src="https://img.shields.io/badge/discourse-browse_forum-red.svg?color=0052FF&labelColor=090422&logo=discourse" /></a>
</p>

## Welcome!

This Prefect collection contains a multiprocess task runner. It is ideal for running CPU-intensive Prefect tasks in parallel, and should be faster than `ConcurrentTaskRunner` in that scenario. However, tasks that are more I/O-bound than CPU-bound may finish more quickly in `ConcurrentTaskRunner`. It's useful in scenarios where you want to spread computation across multiple CPU cores on a single machine without adding heavy dependencies like Dask. This package does not require any extra dependencies beyond what Prefect already installs.

## Getting Started

Install the package by running:
```
pip install prefect-multiprocess
```

Then, use the task runner in your Prefect flows. The task runner only accepts one parameter: `processes`, which controls the number of worker processes to start for running tasks. If not provided, it defaults to the number of CPUs on the host machine.

Examples:

Create one process for every CPU/CPU core
```python
from prefect import flow
from prefect_multiprocess.task_runners import MultiprocessTaskRunner


@flow(task_runner=MultiprocessTaskRunner())
def my_flow():
    ...
```

Customizing the number of processes:
```python
@flow(task_runner=MultiprocessTaskRunner(processes=4))
def my_flow():
    ...
```

### Python setup

Requires an installation of Python 3.8+.

We recommend using a Python virtual environment manager such as pipenv, conda or virtualenv.

These tasks are designed to work with Prefect 2.0. For more information about how to use Prefect, please refer to the [Prefect documentation](https://docs.prefect.io/).

## Limitations

`MultiprocessTaskRunner` uses `cloudpickle` to serialize tasks and return values, so task parameters and returns need to be values that `cloudpickle` can handle.

## Resources

If you encounter any bugs while using `prefect-multiprocess`, feel free to open an issue in the [prefect-multiprocess](https://github.com/rpeden/prefect-multiprocess) repository.

Feel free to ⭐️ or watch [`prefect-multiprocess`](https://github.com/rpeden/prefect-multiprocess) for updates too!

## Development

If you'd like to install a version of `prefect-multiprocess` for development, clone the repository and perform an editable install with `pip`:

```bash
git clone https://github.com/rpeden/prefect-multiprocess.git

cd prefect-multiprocess/

pip install -e ".[dev]"

# Install linting pre-commit hooks
pre-commit install
```
