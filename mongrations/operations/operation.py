from motor.motor_asyncio import AsyncIOMotorClient
from tqdm import tqdm


class Operation:

    def __init__(self):
        super().__init__()

    def accepts_dependency_output(self, phase, destination):
        return True

    def create_default_destination(self, phase):
        raise NotImplemented()

    def invoke(self, client: AsyncIOMotorClient, progress: tqdm, phase):
        raise NotImplemented()

    def needs_source(self):
        return True

    def needs_destination(self):
        return True
