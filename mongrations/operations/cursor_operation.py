import logging
from typing import Optional
from mongrations.io.collection_destination import CollectionDestination
from motor.motor_asyncio import AsyncIOMotorClient
from tqdm import tqdm

from mongrations.io.source import CollectionSource
from mongrations.operations.operation import Operation

logger = logging.getLogger(__name__)

class StreamingAggregationOperation(Operation):
    def __init__(
            self, 
            aggregation: list[dict], 
            batch_size,
            database: Optional[str] = None, 
            collection: Optional[str] = None,
        ):
        super().__init__()
        self._aggregation = aggregation
        self._batch_size = batch_size
        self._database = database
        self._collection = collection

    def accepts_dependency_output(self, phase, destination):
        return True

    def create_default_destination(self, phase):
        col_name = f"mongration-tmp-{phase.sanitized_name()}".replace(".", "-")
        destination = CollectionDestination("mongrations", col_name)
        phase.finalize_with(
            f"Delete temporary {col_name} collection",
            lambda client: client.get_database("mongrations").drop_collection(col_name),
        )
        return destination

    def needs_destination(self):
        # This operation does not need a destination
        return False



    async def invoke(self, client: AsyncIOMotorClient, progress: tqdm, phase):
        logger.debug(f"Starting streaming aggregation operation with batch size {self._batch_size}")
        src = phase.source()
        dest = phase.destination()

        cursor, estimated_total = await src.cursor(client)
        logger.debug(f"Cursor estimated total: {estimated_total} documents")
        progress.total = estimated_total

        dest.hint_total(estimated_total)
        database = self._database
        collection = self._collection
        if database is None or collection is None:
            if isinstance(src, CollectionSource):
                database = src.database
                collection = src.collection
            else:
                raise Exception(
                    f"Incompatible source for aggregation: {src}. Expected CollectionSource."
                )

        collection = client.get_database(database).get_collection(collection)

        sum = 0
        current_batch = list()
            
        async for document in cursor:
            current_batch.append(document)
            current_batch_size = len(current_batch)
            if current_batch_size >= self._batch_size:
                agg = [{"$documents": current_batch}]
                agg.extend(self._aggregation)
                subcursor = collection.aggregate(
                    agg, batchSize=self._batch_size, aggregate=1
                )
                current_batch = list()
                if dest is not None:
                    async for doc in subcursor:
                        await dest.push(doc)
                progress.update(n=current_batch_size)
                sum += len(current_batch)

        if len(current_batch) > 0:
            agg = [{"$documents": current_batch}]
            agg.extend(self._aggregation)
            subcursor = collection.aggregate(
                agg, batchSize=self._batch_size, aggregate=1
            )
            if dest is not None:
                async for doc in subcursor:
                    await dest.push(doc)
            progress.update(n=len(current_batch))
            sum += len(current_batch)
        return sum

    def __str__(self):
        stage_count = len(self._aggregation)
        stage_word = "stage" if stage_count == 1 else "stages"
        return f"Streaming Aggregation ({stage_count} {stage_word})"
