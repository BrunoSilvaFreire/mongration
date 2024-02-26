from motor.motor_asyncio import AsyncIOMotorClient
from typing import Optional


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
    def __init__(self, database: str, collection: str, filter: Optional[dict]):
        self.database = database
        self.collection = collection
        self._filter = filter

    async def cursor(self, client: AsyncIOMotorClient):
        collection = client.get_database(self.database).get_collection(self.collection)
        return collection.find(filter=self._filter), await collection.estimated_document_count(maxTimeMS=2 * 1000)

    def __str__(self):
        return f"{self.database}/{self.collection}"
