import asyncio
import time
from time import sleep

from motor.motor_asyncio import AsyncIOMotorClient
from tqdm import tqdm

from mongrations.engine.engine import Engine
from mongrations.graph import DependencyGraph
from mongrations.phase import Phase


class AsyncIOEngine(Engine):
    def invoke(self, client: AsyncIOMotorClient, graph: DependencyGraph[Phase]):
        index = 0

        async def invoke_operation(operation, progress, source, destination):
            start = time.time()
            current_batch = 0
            batch_size = 64
            generator = operation.invoke(client, progress, source)
            if destination is None:
                async for new_doc in generator:
                    current_batch += 1
                    if current_batch > batch_size:
                        current_batch = 0
                        await asyncio.sleep(0)

            else:
                async for new_doc in generator:
                    current_batch += 1
                    await destination.push(new_doc)
                    if current_batch > batch_size:
                        current_batch = 0
                        await asyncio.sleep(0)
                await destination.close()

            end = time.time()

            duration = end - start
            return duration, current_batch

        async def phase_process(phase, progress):
            operation = phase.operation()
            name = phase.name()
            if operation is None:
                raise Exception(f"Phase {name} has no operation.")
            source = phase.source()
            progress.set_description(f"{name} ({operation} @ {source})")
            duration, total_docs = await invoke_operation(operation, progress, source, phase.destination())
            progress.set_description(name)
            progress.display(f"Phase {name} took {duration:.2f} seconds and wrote {total_docs} docs")
            progress.update()

        operations = list()

        def per_vertex(vertex_index):
            nonlocal index
            phase: Phase = graph[vertex_index]
            progress = tqdm(position=index)

            operations.append(phase_process(phase, progress))

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
