from motor.motor_asyncio import AsyncIOMotorClient
from tqdm import tqdm


class Operation:
    def invoke(self, client: AsyncIOMotorClient, progress: tqdm, phase):
        raise Exception("Not implemented")
