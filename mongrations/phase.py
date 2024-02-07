from motor.motor_asyncio import AsyncIOMotorClient

from mongrations.engine.operation import Operation, PythonOperation
from mongrations.io.collection_destination import CollectionDestination
from mongrations.io.destination import Destination
from mongrations.io.pipe import Pipe
from mongrations.io.source import Source, CollectionSource


class Phase:
    _name: str
    _source: Source
    _destination: Destination
    _dependencies: list
    _operation: Operation

    def __init__(self, name):
        self._name = name
        self._dependencies = list()
        self._source = None
        self._operation = None
        self._destination = None

    def from_collection(self, database: str, collection: str, filter: dict = None):
        self._source = CollectionSource(database, collection, filter)

    def from_phase(self, phase):
        if self == phase:
            raise Exception("Cannot read phase from itself")
        self._dependencies.append(phase)
        pipe = phase.destination()
        if pipe is None:
            pipe = phase._destination = Pipe()
        self._source = pipe

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
        self._destination = CollectionDestination(database, collection)

    def __str__(self):
        return f"Phase(name={self._name})"

    def source(self):
        return self._source

    def destination(self):
        return self._destination
