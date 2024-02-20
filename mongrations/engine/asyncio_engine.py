import asyncio
import time
from time import sleep

from motor.motor_asyncio import AsyncIOMotorClient
from tqdm import tqdm

from mongrations.engine.engine import Engine
from mongrations.graph import DependencyGraph
from mongrations.io.pipe import Pipe
from mongrations.phase import Phase


class AsyncIOEngine(Engine):
    def invoke(self, client: AsyncIOMotorClient, graph: DependencyGraph[Phase]):
        index = 0

        async def invoke_operation(phase: Phase, progress: tqdm):
            start = time.time()
            current_batch = 0
            destination = phase.destination()
            if destination is not None:
                destination.init(client)
            total_processed = await phase.operation().invoke(client, progress, phase)
            await destination.close()

            end = time.time()
            phase.notify_completion()
            duration = end - start
            return duration, total_processed

        async def phase_process(phase, progress):
            operation = phase.operation()
            name = phase.name()
            if operation is None:
                raise Exception(f"Phase {name} has no operation.")
            source = phase.source()
            progress.set_description(f"{name} ({operation} @ {source})")
            duration, total_docs = await invoke_operation(phase, progress)
            progress.set_description(name)
            progress.display(f"Phase {name} took {duration:.2f} seconds and wrote {total_docs} docs")
            progress.update()

        operations = list()
        progress_bars = list[tqdm]()

        def per_vertex(vertex_index):
            nonlocal index
            phase: Phase = graph[vertex_index]
            progress = tqdm(position=index, unit=" docs")

            operations.append(phase_process(phase, progress))
            progress_bars.append(progress)
            index += 1

        def per_edge(src, dst):
            pass

        graph.traverse(
            per_vertex,
            per_edge
        )

        async def fence():
            await asyncio.gather(*operations)

        asyncio.run(fence())
        for bar in progress_bars:
            bar.close()
