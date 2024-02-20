import asyncio

from motor.motor_asyncio import AsyncIOMotorClient

from mongrations.io.collection_destination import CollectionDestination
from mongrations.io.destination import Destination
from mongrations.io.pipe import Pipe
from mongrations.io.source import Source, CollectionSource
from mongrations.operations.aggregation_operation import AggregationOperation
from mongrations.operations.operation import Operation
from mongrations.operations.python_operation import PythonOperation


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
        self._waitingCompletion = list()
        self._isComplete = False

    def from_collection(self, database: str, collection: str, filter: dict = None):
        self._source = CollectionSource(database, collection, filter)

    def from_phase(self, phase):
        if self == phase:
            raise Exception("Cannot read phase from itself")
        self._dependencies.append(phase)
        pipe = phase.destination()
        if pipe is None:
            pipe = phase._destination = Pipe(phase)
        self._source = pipe

    def name(self):
        return self._name

    def dependencies(self):
        return self._dependencies

    def use_python(self, callback):
        self._operation = PythonOperation(callback)

    def use_aggregation(self, database, collection, aggregation):
        self._operation = AggregationOperation(database, collection, aggregation)
        if isinstance(self._source, Pipe):
            src = self._source.incomingPhase
            _ensure_phase_writes_to_collection(collection, database, src)

    def operation(self):
        return self._operation

    def into_temporary(self, database: str):
        pass

    def into_collection(self, database: str, collection: str):
        # TODO: Check if operation is an aggregation, and if is, add an $out stage. A lot fast than python.
        self._destination = CollectionDestination(database, collection)

    def __str__(self):
        return f"Phase(name={self._name})"

    def source(self):
        return self._source

    def destination(self):
        return self._destination

    def complete(self):
        if self._isComplete:
            return
        future = asyncio.Future()
        self._waitingCompletion.append(future)
        return future

    def notify_completion(self):
        self._isComplete = True
        for future in self._waitingCompletion:
            future.set_result(None)


def _ensure_phase_writes_to_collection(collection, database, phase: Phase):
    writer: Destination = phase.destination()
    if writer is None or isinstance(writer, Pipe):
        phase.into_collection(database, collection)

    if isinstance(writer, CollectionDestination):
        if writer.database != database or writer.collection != collection:
            raise Exception(
                f"Phase writes to collection but database/collection mismatch, (expected: {database}, {collection}. actual: {writer.database}, {writer.collection})")
