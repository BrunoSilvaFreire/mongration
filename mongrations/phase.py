from asyncio import Future
from typing import Callable

from mongrations.io.collection_destination import CollectionDestination
from mongrations.io.destination import Destination
from mongrations.io.file_destination import FileDestination
from mongrations.io.pipe import Pipe
from mongrations.io.source import Source, CollectionSource, FileSource, AggregationSource
from mongrations.operations.aggregation_operation import AggregationOperation
from mongrations.operations.cursor_operation import StreamingAggregationOperation
from mongrations.operations.export_operation import ExportOperation
from mongrations.operations.import_operation import ImportOperation
from mongrations.operations.index_operation import IndexOperation
from mongrations.operations.operation import Operation
from mongrations.operations.python_operation import DocumentPythonOperation


class Phase:
    _name: str
    _source: Source
    _destination: Destination
    _dependencies: list["Phase"]
    _operation: Operation
    _finalizers: list[str, Callable]
    _needs_configuration: list["Phase"]

    def __init__(self, name):
        self._name = name
        self._dependencies = list()
        self._source = None
        self._operation = None
        self._destination = None
        self._finalizers = list[str, Callable]()
        self._completionCallbacks = list[str, Callable]()
        self._isComplete = False
        self._must_wait = list()
        self._needs_configuration = list["Phase"]()

    def from_collection(self, database: str, collection: str, filter: dict = None):
        self._source = CollectionSource(database, collection, filter)

    def from_aggregation(self, database: str, collection: str, aggregation: list[dict], options: dict = None):
        self._source = AggregationSource(database, collection, aggregation, options)

    def from_phase(self, source_phase: "Phase"):
        if self == source_phase:
            raise Exception("Cannot read phase from itself")
        self._dependencies.append(source_phase)
        if self._operation is not None:
            self._configure_dependency(source_phase)
        else:
            self._needs_configuration.append(source_phase)

    def _configure_dependency(self, source_phase):
        if self._operation is None:
            raise Exception(
                f"Phase {self._name} doesn't have an operation set, cannot create default destination from {source_phase._name}")
        dest = source_phase.destination()
        if dest is None or not self._operation.accepts_dependency_output(source_phase, dest):
            dest = self._operation.create_default_destination(source_phase)
        source_phase._destination = dest
        dest.pipe_into(source_phase, self)

    def name(self):
        return self._name

    def sanitized_name(self):
        return self._name.replace(' ', '-')

    def on_completed(self, callback):
        self._completionCallbacks.append(callback)

    def finalize_with(self, name, finalizer):
        self._finalizers.append((name, finalizer))

    def dependencies(self):
        return self._dependencies

    def use_python(self, callback):
        self._operation = DocumentPythonOperation(callback)
        self._attempt_auto_configuration()

    def use_aggregation(self, aggregation, options=None):
        self._operation = AggregationOperation(aggregation, options)
        self._attempt_auto_configuration()

    def use_stream(self, sub_aggregation, batch_size=4096):
        self._operation = StreamingAggregationOperation(sub_aggregation, batch_size)
        self._attempt_auto_configuration()

    def import_from(self, file, block, entry_iterator):
        self._source = FileSource(file)
        self._operation = ImportOperation(block, entry_iterator)
        self._attempt_auto_configuration()

    def export_to(self, block):
        self._operation = ExportOperation(block)
        self._attempt_auto_configuration()

    def create_index(self, index, database=None, collection=None):
        if database is not None and collection is not None:
            self._source = CollectionSource(database, collection, None)
        self._operation = IndexOperation(index)
        self._attempt_auto_configuration()

    def _attempt_auto_configuration(self):
        match len(self._needs_configuration):
            case 0:
                return
            case 1:
                self._configure_dependency(self._needs_configuration[0])
            case _:
                print(f"Unable to auto-configure phase {self.name()} because it has multiple dependencies.")
                return

    def operation(self):
        return self._operation

    def into_collection(self, database: str, collection: str):
        # TODO: Check if operation is an aggregation, and if is, add an $out stage. A lot fast than python.
        self._destination = CollectionDestination(database, collection)

    def into_file(self, file_path: str, mode: str = 'w', encoding: str = 'utf-8', batch_size: int = 128):
        self._destination = FileDestination(file_path, mode, encoding, batch_size)

    def __str__(self):
        return f"Phase(name={self._name})"

    def source(self):
        return self._source

    def destination(self):
        return self._destination

    def wait_on(self, future):
        self._must_wait.append(future)

    def wait_for_phase(self, phase):
        completed_future = Future()
        self.wait_on(completed_future)
        phase.on_completed(lambda: completed_future.set_result(None))
        self._dependencies.append(phase)

    async def prepare(self, engine):
        if len(self._must_wait) == 0:
            return
        await engine.wait_all(self._must_wait)

    async def notify_completion(self, num_docs_iterated):
        if self._isComplete:
            return
        self._isComplete = True
        for callback in self._completionCallbacks:
            returned = callback(num_docs_iterated)
            if returned is not None:
                await returned

    async def finalize(self, engine, client):
        to_await = list()
        for name, finalizer in self._finalizers:
            returned = finalizer(client)
            if returned is not None:
                to_await.append(returned)
        if len(to_await) > 0:
            await engine.wait_all(to_await)


def _ensure_phase_writes_to_collection(collection, database, phase: Phase):
    writer: Destination = phase.destination()
    if writer is None or isinstance(writer, Pipe):
        phase.into_collection(database, collection)

    if isinstance(writer, CollectionDestination):
        if writer.database != database or writer.collection != collection:
            raise Exception(
                f"Phase writes to collection but database/collection mismatch, (expected: {database}, {collection}. actual: {writer.database}, {writer.collection})")
