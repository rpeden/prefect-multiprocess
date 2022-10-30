from prefect import flow, task, get_run_logger
from functools import partial


class Webdriver:
    def __init__(self, logger):
        self._logger = logger

    def get_page(self, url):
        self._logger.info(f"fetching {url}")


def logged_webdriver_task(callable_cls):
    old_init = callable_cls.__init__

    def __init__(self, *args, **kwargs):
        logger = get_run_logger()
        self._logger = get_run_logger()
        self._webdriver = Webdriver(logger)
        old_init(self, *args, **kwargs)

    callable_cls.__init__ = __init__
    return task(callable_cls)


@logged_webdriver_task
class PageDownloader:
    def get_page(self, url):
        self(url)

    def __call__(self, url):
        self._logger.info("calling the class as a prefect task!")
        self._webdriver.get_page(url)


@flow
def test_flow():
    downloader = PageDownloader()
    downloader.get_page("https://www.something.com")
    downloader.get_page("https://www.somethingelse.com")


@task
class MyTask:
    def __call__(self):
        self.logger.info("calling the class as a prefect task!")


@flow
class MyFlow:
    def __init__(self):
        self.logger = get_run_logger()

    def do_something(self):
        self.logger.info("testing")

    def __call__(self):
        MyTask()
        MyTask()
        MyTask()
        self.logger.info("calling the class as a prefect flow!")


from pydantic import SecretStr


@flow
def flow1():
    secret = SecretStr("password")
    flow2(secret)


@flow
def flow2(secret: SecretStr):
    logger = get_run_logger()
    logger.info(f"secret: {secret}")
    secret.get_secret_value()
    logger.info(f"unwrapped secret: {secret.get_secret_value()}")


from prefect.deployments import Deployment

Deployment()
if __name__ == "__main__":
    flow1()
