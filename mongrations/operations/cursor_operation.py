from motor.motor_asyncio import AsyncIOMotorClient
from tqdm import tqdm

from mongrations.io.source import CollectionSource
from mongrations.operations.operation import Operation


class StreamingAggregationOperation(Operation):
    def __init__(self, aggregation: list[dict], batch_size):
        super().__init__()
        self._aggregation = aggregation
        self._batch_size = batch_size

    def accepts_dependency_output(self, phase, destination):
        # This operation does not rely on destination output
        return False

    def create_default_destination(self, phase):
        # This operation does not produce a destination
        return None

    def needs_destination(self):
        # This operation does not need a destination
        return False

    async def invoke(self, client: AsyncIOMotorClient, progress: tqdm, phase):
        src = phase.source()
        dest = phase.destination()

        cursor, estimated_total = await src.cursor(client)
        progress.total = estimated_total

        dest.hint_total(estimated_total)
        if isinstance(src, CollectionSource):
            database = src.database
            collection = src.collection
        else:
            raise Exception(f"Incompatible source for aggregation: {src}. Expected CollectionSource.")

        collection = client.get_database(database).get_collection(collection)

        sum = 0
        current_batch = list()
        async for document in cursor:
            current_batch.append(document)
            current_batch_size = len(current_batch)
            if current_batch_size >= self._batch_size:
                agg = [
                    {"$documents": current_batch}
                ]
                agg.extend(self._aggregation)
                subcursor = collection.aggregate(agg, batchSize=self._batch_size, aggregate=1)
                current_batch = list()
                if dest is not None:
                    async for doc in subcursor:
                        await dest.push(doc)
                progress.update(n=current_batch_size)

            sum += 1

        return sum

    def __str__(self):
        return "CursorOperation"
