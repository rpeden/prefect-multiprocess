from . import _version
from .task_runners import MultiprocessTaskRunner # noqas

__version__ = _version.get_versions()["version"]
