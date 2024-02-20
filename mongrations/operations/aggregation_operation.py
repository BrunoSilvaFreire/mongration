import asyncio
from typing import Any

from motor.motor_asyncio import AsyncIOMotorClient
from tqdm import tqdm
from mongrations.operations.operation import Operation


class AggregationOperation(Operation):
    def __init__(self, database: str, collection: str, aggregation: list[dict[str, Any]]):
        self._aggregation = aggregation
        self._database = database
        self._collection = collection

    async def invoke(self, client: AsyncIOMotorClient, progress: tqdm, phase):
        for dependency in phase.dependencies():
            await dependency.complete()
        # Assuming `source.collection_name` gives the name of the source collection
        collection = client.get_database(self._database).get_collection(self._collection)

        # Start the aggregation
        cursor = collection.aggregate(self._aggregation)

        # Initialize progress bar
        # Process documents
        sum = 0
        async for doc in cursor:
            progress.update()  # Update progress for each document
            sum += 1

        return sum
