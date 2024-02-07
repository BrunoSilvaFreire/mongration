from motor.motor_asyncio import AsyncIOMotorClient

from mongrations.engine.operation import Operation, PythonOperation
from mongrations.source import Source, CollectionSource, PhaseSource


class Phase:
    _name: str
    _source: Source
    _dependencies: list
    _operation: Operation

    def __init__(self, name):
        self._name = name
        self._dependencies = list()
        self._source = None
        self._operation = None

    def from_collection(self, database: str, collection: str, filter: dict = None):
        self._source = CollectionSource(database, collection, filter)

    def from_phase(self, phase):
        if self == phase:
            raise Exception("Cannot read phase from itself")
        self._dependencies.append(phase)
        self._source = PhaseSource(phase)

    def name(self):
        return self._name

    def dependencies(self):
        return self._dependencies

    def use_python(self, callback):
        self._operation = PythonOperation(callback)

    def operation(self):
        return self._operation

    def into_temporary(self, database: str):
        pass

    def into_collection(self, database: str, collection: str):
        pass

    def __str__(self):
        return f"Phase(name={self._name})"

    def source(self):
        return self._source
