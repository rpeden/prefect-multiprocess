from asyncio import BaseEventLoop
from typing import cast

import anyio
from anyio.lowlevel import current_token


class Event:
    def __init__(self):
        self._loop: BaseEventLoop = cast(BaseEventLoop, current_token())
        self._event = anyio.Event()

    def set(self):
        self._loop.call_soon_threadsafe(self._event.set)

    async def wait(self):
        await self._event.wait()
