from mongrations.io.source import FileSource
from mongrations.misc.not_supported import NotSupported
from mongrations.operations.python_operation import AbstractPythonOperation


class ImportOperation(AbstractPythonOperation):
    def __init__(self, block, entry_iterator):
        super().__init__(block)
        self.entry_iterator = entry_iterator

    def accepts_dependency_output(self, phase, destination):
        raise NotSupported("Not supported, import operation should not have any phase as it's source.")

    async def _iterate(self, client, phase, progress):
        source = phase.source()
        destination = phase.destination()
        if not isinstance(source, FileSource):
            return
        destination.hint_total(None)
        async for doc in self.entry_iterator(source.path):
            yield self._block(doc)
            progress.update()
