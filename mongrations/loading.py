import importlib
import sys
from pathlib import Path

from motor.motor_asyncio import AsyncIOMotorClient

from mongrations.engine.sequential_engine import SequentialEngine
from mongrations.graph import DependencyGraph
from mongrations.mongration import Mongration
from mongrations.phase import Phase


def load_mongration_script(script_path: Path):
    absolute = script_path.absolute()
    to_load = script_path.name.removesuffix(".py")

    dirname = str(absolute.parent)
    sys.path = [dirname] + sys.path
    mongration_module = importlib.import_module(to_load)
    if hasattr(mongration_module, "mongration"):
        return mongration_module.mongration
    else:
        print(f"No 'mongration' function found in {script_path}.")
        return None


def build_dependency_graph(phases: list[Phase]) -> DependencyGraph[Phase]:
    dependency_graph = DependencyGraph()
    phase_id_cache = dict()
    for phase in phases:
        phase_id_cache[phase] = dependency_graph.add(phase)

    for phase in phases:
        src = phase_id_cache[phase]
        for dependency in phase.dependencies():
            dest = phase_id_cache[dependency]
            dependency_graph.add_dependency(src, dest)

    return dependency_graph


def run_mongration(args):
    mongration_script = args.mongration
    mongration_path = Path(mongration_script)
    if not mongration_path.exists() or not mongration_path.is_file():
        print(f"The specified mongration script does not exist: {mongration_script}")
        return

    mongration_function = load_mongration_script(mongration_path)
    if mongration_function:
        # Assuming Mongration class is defined somewhere within this script or imported
        mongration_instance = Mongration()
        mongration_function(mongration_instance)

        engine = SequentialEngine()
        phases = mongration_instance.phases()
        graph = build_dependency_graph(phases)
        client = AsyncIOMotorClient("mongodb://root:letmein@localhost:27017")
        engine.invoke(client, graph)

        print("Mongration process completed successfully.")
    else:
        print("Failed to load the mongration function.")
