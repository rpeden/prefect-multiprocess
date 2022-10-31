from . import _version
from .task_runners import MultiprocessTaskRunner  # noqa

__version__ = _version.get_versions()["version"]
