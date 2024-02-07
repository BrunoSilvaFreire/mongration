import asyncio
import time
from time import sleep

from motor.motor_asyncio import AsyncIOMotorClient
from tqdm import tqdm

from mongrations.engine.engine import Engine
from mongrations.graph import DependencyGraph
from mongrations.phase import Phase


class SequentialEngine(Engine):
    def invoke(self, client: AsyncIOMotorClient, graph: DependencyGraph[Phase]):
        bar = tqdm(total=graph.get_size(), unit="phases", position=0, leave=True)

        index = 0

        async def invoke_operation(operation, secondary, source):
            start = time.time()
            total_docs = 0
            generator = operation.invoke(client, secondary, source)

            async for new_doc in generator:
                total_docs += 1
            end = time.time()
            duration = end - start
            return duration, total_docs

        def per_vertex(vertex_index):
            nonlocal index
            phase: Phase = graph[vertex_index]
            secondary = tqdm(position=index + 1)
            operation = phase.operation()
            name = phase.name()
            if operation is None:
                raise Exception(f"Phase {name} has no operation.")
            source = phase.source()
            bar.set_description(f"{name} ({operation} @ {source})")
            duration, total_docs = asyncio.run(invoke_operation(operation, secondary, source))
            bar.set_description(name)
            bar.display(f"Phase {name} took {duration:.2f} seconds and wrote {total_docs} docs")
            bar.update()
            index += 1

        def per_edge(src, dst):
            pass

        sleep(0.5)
        graph.traverse(
            per_vertex,
            per_edge
        )
        sleep(0.5)
