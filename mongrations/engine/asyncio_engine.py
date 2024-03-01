import asyncio
import time

from motor.motor_asyncio import AsyncIOMotorClient
from tqdm import tqdm

from mongrations.engine.engine import Engine
from mongrations.phase import Phase


class AsyncIOEngine(Engine):

    async def invoke(self, client, mongration, graph):

        async def invoke_operation(phase: Phase, progress: tqdm):
            start = time.time()
            destination = phase.destination()
            if destination is not None:
                destination.init(client)
            total_processed = await phase.operation().invoke(client, progress, phase)
            if destination is not None:
                await destination.close()

            end = time.time()
            await phase.notify_completion(total_processed)
            duration = end - start
            return duration, total_processed

        async def phase_process(phase, progress):
            operation = phase.operation()
            name = phase.name()
            if operation is None:
                raise Exception(f"Phase {name} has no operation.")

            source = phase.source()
            progress.set_description(
                f"{name} (op: {operation}, src: {source}, dst: {phase.destination()}): Preparing...")
            await phase.prepare(self)
            progress.set_description(f"{name} (op: {operation}, src: {source}, dst: {phase.destination()}): Running")

            try:
                duration, total_docs = await invoke_operation(phase, progress)
            except Exception as e:
                raise Exception(f"An error occoured while invoking operation on phase {name}") from e
            progress.set_description(name)
            progress.display(f"Phase {name} took {duration:.2f} seconds and wrote {total_docs} docs")
            progress.update()

        operations = list()
        progress_bars = list[tqdm]()
        for i in range(graph.get_size()):
            progress_bars.append(tqdm(position=i, unit=" docs"))

        def per_vertex(vertex_index):
            phase: Phase = graph[vertex_index]
            progress = progress_bars[vertex_index]

            operations.append(phase_process(phase, progress))

        def per_edge(src, dst):
            pass

        graph.traverse(per_vertex, per_edge)
        await asyncio.gather(*operations)

        for i in range(graph.get_size()):
            progress_bars[i].display("Finalizing...")
            await graph[i].finalize(self, client)

        for bar in progress_bars:
            bar.close()

    def start(self, entrypoint):
        return asyncio.run(entrypoint)

    async def wait_all(self, futures: list):
        return await asyncio.gather(*futures)
