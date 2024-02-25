class Destination:
    async def push(self, item):
        pass

    async def close(self):
        pass

    def init(self, client):
        pass

    def hint_total(self, estimated_total):
        pass
    def pipe_into(self, source: "mongrations.phase.Phase", destination: "mongrations.phase.Phase"):
        pass