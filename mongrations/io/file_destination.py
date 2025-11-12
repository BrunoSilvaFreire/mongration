import asyncio
import json
import pathlib
from queue import Queue
from typing import Any
from bson import json_util

from mongrations.io.destination import Destination


class FileDestination(Destination):
    def __init__(self, file_path: str, mode: str = 'w', encoding: str = 'utf-8', batch_size: int = 128):
        """
        Initialize the FileDestination with file handling parameters.

        :param file_path: Path to the file where data will be written.
        :param mode: Mode in which the file should be opened ('w' for writing, 'a' for appending).
        :param encoding: Encoding of the file.
        :param batch_size: Number of items to accumulate before flushing to file.
        """
        self.file_path = file_path
        self.mode = mode
        self.encoding = encoding
        self.batch_size = batch_size
        self.buffer = Queue()
        self.file_handle = None
        self.first_item = True

    def init(self, client=None):
        """
        Asynchronously opens the file with the specified mode and encoding.
        """
        pathlib.Path(self.file_path).parent.mkdir(exist_ok=True, parents=True)
        self.file_handle = open(self.file_path, self.mode, encoding=self.encoding)

        self.file_handle.write('[')
        self.first_item = True

    async def push(self, item: Any):
        """
        Asynchronously adds an item to the buffer. Flushes the buffer to file if the batch size is reached.
        """
        self.buffer.put(item)
        if self.buffer.qsize() >= self.batch_size:
            await self._flush()

    async def _flush(self):
        """
        Asynchronously writes items from the buffer to the file.
        Uses json_util from bson to handle MongoDB-specific types like ObjectId.
        """
        while not self.buffer.empty():
            item = self.buffer.get()

            # Add comma separator before each item except the first
            if not self.first_item:
                self.file_handle.write(',\n')
            else:
                self.file_handle.write('\n')
                self.first_item = False

            # Use bson.json_util to properly serialize MongoDB types
            self.file_handle.write(json_util.dumps(item))
        await asyncio.sleep(0)  # Yield control to allow other coroutines to run

    async def close(self):
        await self._flush()
        self.file_handle.write('\n]')
        self.file_handle.close()

    def __str__(self):
        return f"FileDestination({self.file_path})"
