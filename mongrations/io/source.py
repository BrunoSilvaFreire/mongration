from typing import Optional

from motor.motor_asyncio import AsyncIOMotorClient


class Source:
    pass

    def cursor(self, client: AsyncIOMotorClient):
        pass


class CollectionSource(Source):
    def __init__(self, database: str, collection: str, filter: Optional[dict]):
        self.database = database
        self.collection = collection
        self._filter = filter

    async def cursor(self, client: AsyncIOMotorClient):
        collection = client.get_database(self.database).get_collection(self.collection)
        return collection.find(filter=self._filter), await collection.estimated_document_count(maxTimeMS=2 * 1000)

    def __str__(self):
        return f"{self.database}/{self.collection}"
