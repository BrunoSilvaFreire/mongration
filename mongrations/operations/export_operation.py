from mongrations.operations.python_operation import AbstractPythonOperation


class ExportOperation(AbstractPythonOperation):
    def __init__(self, block):
        super().__init__(block)

    def needs_destination(self):
        return False

    async def _setup(self, client, phase, progress):
        source = phase.source()
        cursor, estimated_total = await source.cursor(client)
        progress.total = estimated_total

        async for doc in cursor:
            await self._block(doc)
            yield None
            progress.update()
