import importlib
import sys
from pathlib import Path

from mongrations.engine.asyncio_engine import AsyncIOEngine
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


def run_mongration(args):
    mongration_script = args.mongration
    mongration_path = Path(mongration_script)
    if not mongration_path.exists() or not mongration_path.is_file():
        print(f"The specified mongration script does not exist: {mongration_script}")
        return

    mongration_function = load_mongration_script(mongration_path)
    if mongration_function:
        engine = AsyncIOEngine()
        engine.invoke(lambda: load_mongration(mongration_function))

        print("Mongration process completed successfully.")
    else:
        print("Failed to load the mongration function.")


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


def _build_list(phases):
    return ", ".join([f'"{phase.name()}"' for phase in phases])


def load_mongration(mongration_function):
    mongration_instance = Mongration()
    mongration_function(mongration_instance)

    phases = mongration_instance.phases()
    phases_without_source = list(filter(lambda phase: phase.source() is None, phases))
    phases_without_dest = list(filter(lambda phase: phase.destination() is None, phases))
    if len(phases_without_source) > 0 or len(phases_without_dest) > 0:
        no_source_msg = _build_list(phases_without_source)
        no_dest_msg = _build_list(phases_without_dest)
        raise Exception(
            f"Some phases are misconfigured. Phases without sources: [{no_source_msg}], Phases without destinations: [{no_dest_msg}].")
    graph = build_dependency_graph(phases)
    graph.print_to_terminal(
        circle_radius=8,
        padding=10,
        name_selector=lambda phase: phase.name(),
    )
    return mongration_instance, graph
