from typing import Any

from motor.motor_asyncio import AsyncIOMotorClient
from tqdm import tqdm

from mongrations.io.collection_destination import CollectionDestination
from mongrations.io.source import CollectionSource
from mongrations.operations.operation import Operation


class AggregationOperation(Operation):
    def __init__(self, aggregation: list[dict[str, Any]]):
        super().__init__()
        self._aggregation = aggregation

    def accepts_dependency_output(self, phase, destination):
        return isinstance(destination, CollectionDestination)

    def create_default_destination(self, phase):
        col_name = f"mongration-tmp-{phase.sanitized_name()}"
        destination = CollectionDestination("mongrations", col_name)
        phase.finalize_with(
            f"Delete temporary {col_name} collection",
            lambda client: client.get_database("mongrations").drop_collection(col_name)
        )
        return destination

    async def invoke(self, client: AsyncIOMotorClient, progress: tqdm, phase):
        # for dependency in phase.dependencies():
        #     await dependency.complete()
        src = phase.source()
        dest = phase.destination()
        if isinstance(src, CollectionSource):
            database = src.database
            collection = src.collection
        else:
            raise Exception(f"Incompatible source for aggregation: {src}. Expected CollectionSource.")
        # Assuming `source.collection_name` gives the name of the source collection
        collection = client.get_database(database).get_collection(collection)
        agg = self._aggregation
        if isinstance(dest, CollectionDestination):
            last_phase: dict = agg[len(agg) - 1]
            if last_phase.get("$out", None) is None:
                agg.append({

                "$out": {"db": dest.database, "coll":dest.collection}
                })

        # Start the aggregation
        cursor = collection.aggregate(agg)

        # Initialize progress bar
        # Process documents
        sum = 0
        async for doc in cursor:
            progress.update()  # Update progress for each document
            sum += 1

        return sum

    def __str__(self):
        return "Aggregation"
