import asyncio
import logging
from motor.motor_asyncio import AsyncIOMotorClient

from mongrations.io.destination import Destination
from mongrations.io.source import DocumentSource

logger = logging.getLogger(__name__)


class Pipe(DocumentSource, Destination):
    def __init__(self):
        self._queue = asyncio.Queue()
        self._end_of_pipe = False
        self._has_been_hinted = False
        self._hinted_future = asyncio.Future()
        self._total_hint = None
        self._items_pushed = 0
        self._items_consumed = 0
        logger.debug(f"Pipe created: {id(self)}")

    async def push(self, item):
        self._items_pushed += 1
        logger.debug(f"Pipe {id(self)}: Pushing item #{self._items_pushed} to queue (queue size: {self._queue.qsize()})")
        await self._queue.put(item)
        logger.debug(f"Pipe {id(self)}: Item #{self._items_pushed} pushed successfully")

    async def close(self):
        logger.debug(f"Pipe {id(self)}: Closing pipe (pushed {self._items_pushed} items, end_of_pipe={self._end_of_pipe})")
        self._end_of_pipe = True
        # Add a sentinel value to unblock the cursor if it's waiting for items.
        await self._queue.put(None)
        logger.debug(f"Pipe {id(self)}: Sentinel value added to queue")

    async def cursor(self, client: AsyncIOMotorClient):
        logger.debug(f"Pipe {id(self)}: cursor() called, has_been_hinted={self._has_been_hinted}")
        if not self._has_been_hinted:
            logger.debug(f"Pipe {id(self)}: Waiting for hint...")
            self._total_hint = await self._hinted_future
            logger.debug(f"Pipe {id(self)}: Received hint: {self._total_hint}")
        logger.debug(f"Pipe {id(self)}: Returning cursor and hint {self._total_hint}")
        return self._cursor(), self._total_hint

    async def _cursor(self):
        logger.debug(f"Pipe {id(self)}: _cursor() generator starting")
        while not (self._end_of_pipe and self._queue.empty()):
            logger.debug(f"Pipe {id(self)}: Waiting for item from queue (end_of_pipe={self._end_of_pipe}, queue_empty={self._queue.empty()}, queue_size={self._queue.qsize()})")
            item = await self._queue.get()
            logger.debug(f"Pipe {id(self)}: Got item from queue: {item is None and 'SENTINEL' or 'DATA'}")
            if item is None:
                logger.debug(f"Pipe {id(self)}: Received sentinel, breaking")
                break
            self._items_consumed += 1
            logger.debug(f"Pipe {id(self)}: Yielding item #{self._items_consumed}")
            yield item
        logger.debug(f"Pipe {id(self)}: _cursor() generator completed (consumed {self._items_consumed} items)")

    def pipe_into(self, src, dst):
        logger.debug(f"Pipe {id(self)}: Piping from {src} to {dst}")
        src._destination = self
        dst._source = self

    def __str__(self):
        return f"Pipe({id(self)})"

    def hint_total(self, estimated_total):
        logger.debug(f"Pipe {id(self)}: hint_total called with {estimated_total}")
        self._has_been_hinted = True
        self._hinted_future.set_result(estimated_total)
        logger.debug(f"Pipe {id(self)}: hint_total set successfully")
