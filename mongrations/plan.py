from enum import Enum


class MongrationStatus(Enum):
    ABSENT = 0
    WORK_IN_PROGRESS = 1
    FAILED = 2
    COMPLETED = 3

    @classmethod
    def by_name(cls, name) -> "MongrationStatus":
        return cls._member_map_.get(name, None)


class MongrationState:
    index: int
    name: str
    status: MongrationStatus

    def __init__(self, index: int, name: str, status: MongrationStatus):
        self.index = index
        self.name = name
        self.status = status

    async def _set_status(self, collection, status: MongrationStatus):
        await collection.update_one(
            {"_id": self.index},
            {"$set": {"status": status}},
            upsert=True
        )
        self.status = status

    async def work_in_progress(self, collection):
        await self._set_status(collection, MongrationStatus.WORK_IN_PROGRESS)
