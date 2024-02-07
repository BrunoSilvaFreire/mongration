from motor.motor_asyncio import AsyncIOMotorClient
from mongrations.graph import DependencyGraph
from mongrations.phase import Phase


class Engine:
    def invoke(self, client: AsyncIOMotorClient, graph: DependencyGraph[Phase]):
        pass
