import importlib
import sys
from pathlib import Path

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


def _build_list(phases):
    return ", ".join([f'"{phase.name()}"' for phase in phases])


def load_mongration(name, mongration_function):
    mongration_instance = Mongration(name)
    mongration_function(mongration_instance)

    phases = mongration_instance.phases()
    phases_without_source = list(filter(lambda phase: phase.operation().needs_source() and phase.source() is None, phases))
    phases_without_dest = list(filter(lambda phase:  phase.operation().needs_destination() and phase.destination() is None, phases))
    if len(phases_without_source) > 0 or len(phases_without_dest) > 0:
        no_source_msg = _build_list(phases_without_source)
        no_dest_msg = _build_list(phases_without_dest)
        raise Exception(
            f"Some phases are misconfigured. Phases without sources: [{no_source_msg}], Phases without destinations: [{no_dest_msg}].")
    return mongration_instance
