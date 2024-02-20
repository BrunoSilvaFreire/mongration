from queue import Queue

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pymongo import UpdateOne, InsertOne

from mongrations.io.destination import Destination


class CollectionDestination(Destination):
    _cached_collection: AsyncIOMotorCollection

    def __init__(self, database, collection, batch_size=128):
        self._cached_collection = None
        self.database = database
        self.collection = collection
        self.batch_size = batch_size
        self.buffer = Queue()

    def init(self, client: AsyncIOMotorClient):
        self._cached_collection = client.get_database(self.database).get_collection(self.collection)

    async def push(self, item):
        self.buffer.put(item)
        if self.buffer.qsize() >= self.batch_size:
            await self._flush()

    async def _flush(self):
        requests = list()
        while not self.buffer.empty():
            entry = self.buffer.get()
            requests.append(UpdateOne({
                "_id": entry["_id"]
            }, {
                "$set": entry
            }, upsert=True))
        self._cached_collection.bulk_write(requests)

    async def close(self):
        pass
