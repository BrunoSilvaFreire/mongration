import asyncio
import logging
from typing import AsyncIterable

from motor.motor_asyncio import AsyncIOMotorClient
from tqdm import tqdm

from mongrations.io.pipe import Pipe
from mongrations.operations.operation import Operation

logger = logging.getLogger(__name__)


class AbstractPythonOperation(Operation):
    def __init__(self, block):
        super().__init__()
        self._block = block

    async def _process(self, cursor, progress):
        async for doc in cursor:
            yield self._block(doc)
            progress.update()

    def create_default_destination(self, phase):
        return Pipe()

    def _iterate(self, client, phase, progress) -> AsyncIterable:
        raise NotImplemented()

    async def invoke(self, client: AsyncIOMotorClient, progress: tqdm, phase):
        batch_size = 64

        destination = phase.destination()

        increment = 0
        current_batch = 0
        iterator = self._iterate(client, phase, progress)
        if destination is None:
            async for _ in iterator:
                current_batch = await self._notify_batch(batch_size, current_batch)
                increment += 1
        else:
            async for new_doc in iterator:

                if new_doc is not None:
                    await destination.push(new_doc)
                current_batch = await self._notify_batch(batch_size, current_batch)
                increment += 1

        return increment

    async def _notify_batch(self, batch_size, current_batch):
        if current_batch > batch_size:
            await asyncio.sleep(0)
            return 0
        else:
            return current_batch + 1

    def __str__(self):
        block_name = getattr(self._block, '__name__', 'lambda')
        return f"Python ({block_name})"


class DocumentPythonOperation(AbstractPythonOperation):
    def __init__(self, block):
        super().__init__(block)

    async def _iterate(self, client, phase, progress):
        source = phase.source()
        destination = phase.destination()
        logger.debug(f"DocumentPythonOperation._iterate: Getting cursor from source {source}")
        cursor, estimated_total = await source.cursor(client)
        logger.debug(f"DocumentPythonOperation._iterate: Got cursor, estimated_total={estimated_total}")
        progress.total = estimated_total

        logger.debug(f"DocumentPythonOperation._iterate: Calling hint_total({estimated_total}) on destination {destination}")
        destination.hint_total(estimated_total)
        logger.debug(f"DocumentPythonOperation._iterate: Starting to iterate over cursor")
        doc_count = 0
        async for doc in cursor:
            doc_count += 1
            logger.debug(f"DocumentPythonOperation._iterate: Processing document #{doc_count}")
            yield self._block(doc)
            progress.update()
        logger.debug(f"DocumentPythonOperation._iterate: Finished iterating, processed {doc_count} documents")


class GeneratorPythonOperation(AbstractPythonOperation):
    """Python operation that generates documents without needing a source."""
    def __init__(self, block):
        super().__init__(block)

    def needs_source(self):
        return False

    async def _iterate(self, client, phase, progress):
        destination = phase.destination()
        # Call the block function once to generate a document
        # The block should return a single document, a list of documents, or a generator
        result = self._block(None)
        
        # Check if result is iterable (duck typing)
        # Exclude strings and dicts as they're iterable but represent single documents
        from collections.abc import Iterable
        if isinstance(result, Iterable) and not isinstance(result, (str, dict)):
            # It's an iterable - consume it
            docs = list(result)
            progress.total = len(docs)
            destination.hint_total(len(docs))
            for doc in docs:
                yield doc
                progress.update()
        else:
            # Single document
            progress.total = 1
            destination.hint_total(1)
            yield result
            progress.update()

    def __str__(self):
        block_name = getattr(self._block, '__name__', 'lambda')
        return f"Generate ({block_name})"

