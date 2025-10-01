import logging
from enum import Enum

from mongrations.phase import Phase

logger = logging.getLogger(__name__)


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
    phases_ran: list[dict]

    def __init__(self, index: int, name: str, status: MongrationStatus):
        self.index = index
        self.name = name
        self.status = status
        self.phases_ran = list()

    async def _set_status(self, collection, status: MongrationStatus):
        logger.debug(f"Setting mongration {self.name} (index: {self.index}) status from {self.status.name} to {status.name}")
        await collection.update_one(
            {"_id": self.index},
            {
                "$set": {
                    "name": self.name,
                    "status": status.name
                }
            },
            upsert=True
        )
        self.status = status
        logger.info(f"Mongration {self.name} status updated to {status.name}")

    async def work_in_progress(self, collection):
        await self._set_status(collection, MongrationStatus.WORK_IN_PROGRESS)

    async def notify_phase_completed(self, collection, phase: Phase, num_documents_iterated: int):
        logger.info(f"Phase '{phase.name()}' completed for mongration {self.name}, processed {num_documents_iterated} documents")
        phase_meta = {
            "phase": phase.name(),
            "num_documents_iterated": num_documents_iterated
        }
        self.phases_ran.append(phase_meta)
        await collection.update_one(
            {"_id": self.index},
            {
                "$addToSet": {
                    "phases_ran": phase_meta
                },
                "$set": {
                    "name": self.name,
                }
            },
            upsert=True
        )

    async def completed(self, collection):
        await self._set_status(collection, MongrationStatus.COMPLETED)

    async def failed(self, collection):
        await self._set_status(collection, MongrationStatus.FAILED)
