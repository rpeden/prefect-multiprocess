from . import _version
from .task_runners import MultiprocessTaskRunner, AnyioMultiprocessTaskRunner  # noqas

__version__ = _version.get_versions()["version"]
