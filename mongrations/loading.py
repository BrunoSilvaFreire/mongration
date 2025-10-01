import importlib
import logging
import sys
from pathlib import Path

from mongrations.graph import DependencyGraph
from mongrations.mongration import Mongration
from mongrations.phase import Phase

logger = logging.getLogger(__name__)


def load_mongration_script(script_path: Path):
    logger.debug(f"Attempting to load mongration script from {script_path}")
    absolute = script_path.absolute()
    to_load = script_path.name.removesuffix(".py")

    dirname = str(absolute.parent)
    sys.path = [dirname] + sys.path
    logger.debug(f"Importing module: {to_load}")
    mongration_module = importlib.import_module(to_load)
    if hasattr(mongration_module, "mongration"):
        logger.debug(f"Found 'mongration' function in {script_path}")
        return mongration_module.mongration
    else:
        logger.warning(f"No 'mongration' function found in {script_path}")
        return None


def build_dependency_graph(phases: list[Phase]) -> DependencyGraph[Phase]:
    logger.debug(f"Building dependency graph for {len(phases)} phases")
    dependency_graph = DependencyGraph()
    phase_id_cache = dict()
    for phase in phases:
        phase_id_cache[phase] = dependency_graph.add(phase)

    for phase in phases:
        src = phase_id_cache[phase]
        for dependency in phase.dependencies():
            dest = phase_id_cache[dependency]
            dependency_graph.add_dependency(src, dest)
            logger.debug(f"Added dependency: {phase.name()} depends on {dependency.name()}")

    logger.debug(f"Dependency graph built with {dependency_graph.get_size()} nodes")
    return dependency_graph


def _build_list(phases):
    return ", ".join([f'"{phase.name()}"' for phase in phases])


def load_mongration_from_script(name, mongration_function):
    logger.debug(f"Loading mongration '{name}' from script function")
    mongration_instance = Mongration(name)
    mongration_function(mongration_instance)

    phases = mongration_instance.phases()
    logger.debug(f"Mongration '{name}' has {len(phases)} phases")
    for phase in phases:
        if phase.operation() is None:
            logger.error(f"Phase {phase.name()} has no operation set")
            raise Exception(f"Phase {phase.name()} has no operation set.")
    phases_without_source = list(filter(lambda phase: phase.operation().needs_source() and phase.source() is None, phases))
    phases_without_dest = list(filter(lambda phase:  phase.operation().needs_destination() and phase.destination() is None, phases))
    if len(phases_without_source) > 0 or len(phases_without_dest) > 0:
        no_source_msg = _build_list(phases_without_source)
        no_dest_msg = _build_list(phases_without_dest)
        logger.error(f"Mongration '{name}' has misconfigured phases. Without sources: [{no_source_msg}], Without destinations: [{no_dest_msg}]")
        raise Exception(
            f"Some phases are misconfigured. Phases without sources: [{no_source_msg}], Phases without destinations: [{no_dest_msg}].")
    logger.info(f"Successfully loaded mongration '{name}' with {len(phases)} phases")
    return mongration_instance
