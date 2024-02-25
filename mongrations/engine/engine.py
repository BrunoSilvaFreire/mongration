from motor.motor_asyncio import AsyncIOMotorClient

from mongrations.graph import DependencyGraph
from mongrations.phase import Phase


class Engine:
    def invoke(self, mongration_function):
        pass

    async def wait_all(self, futures: list):
        pass
