from typing import Optional
from motor.motor_asyncio import AsyncIOMotorClient


class Source:
    pass


class FileSource(Source):
    def __init__(self, path):
        self.path = path

    def __str__(self):
        return f"file://{self.path}"


class DocumentSource(Source):

    def cursor(self, client: AsyncIOMotorClient):
        pass


class CollectionSource(DocumentSource):
    def __init__(self, database: str, collection: str, query_filter: Optional[dict]):
        self.database = database
        self.collection = collection
        self._filter = query_filter

    async def cursor(self, client: AsyncIOMotorClient):
        collection = client.get_database(self.database).get_collection(self.collection)
        return collection.find(filter=self._filter), await collection.estimated_document_count(maxTimeMS=2 * 1000)

    def __str__(self):
        return f"{self.database}/{self.collection}"


class AggregationSource(DocumentSource):
    def __init__(self, database: str, collection: str, pipeline: list[dict]):
        self.database = database
        self.collection = collection
        self.pipeline = pipeline

    async def cursor(self, client: AsyncIOMotorClient):
        collection = client.get_database(self.database).get_collection(self.collection)
        cursor = collection.aggregate(self.pipeline)
        return cursor, await collection.estimated_document_count(maxTimeMS=2 * 1000)

    def __str__(self):
        return f"{self.database}/{self.collection}"
