import logging
import os
import traceback
from pathlib import Path
from typing import Optional

from mongrations.mongration import Mongration
from motor.motor_asyncio import AsyncIOMotorClient

from mongrations.engine.asyncio_engine import AsyncIOEngine
from mongrations.loading import load_mongration_script, load_mongration_from_script, build_dependency_graph
from mongrations.plan import MongrationStatus, MongrationState

logger = logging.getLogger(__name__)


class MongrationProgram:

    async def _fetch_status(self, state_collection):
        states = list()

        async for doc in state_collection.find():
            states.append(MongrationState(
                index=doc["_id"],
                name=doc['name'],
                status=MongrationStatus.by_name(doc.get("status", "ABSENT"))
                )
            )
        states.sort(key=lambda state: state.index, reverse=True)
        return states

    def run(self, args):

        engine = AsyncIOEngine()
        engine.start(self._main(args, engine))

    def manipulate(self, args):
        engine = AsyncIOEngine()
        engine.start(self._manipulate(args, engine))

    async def _manipulate(self, args, engine):
        
        url = args.url
        mongration_script = args.mongration
        logger.info(f"Connecting to mongodb at {url}...")
        client = AsyncIOMotorClient(url)
        logger.info("Successfully connected to mongodb")
        mongration_path = Path(mongration_script)
        mongration = self.load_mongration_at_path(mongration_path)
        if mongration is None:
            return
        state_collection = client.get_database("mongrations").get_collection("state")

        states = await self._fetch_status(state_collection)
        states.sort(key=lambda state: state.index)

        states_by_name: dict[dict, MongrationState] = {state.name: state for state in states}
        if mongration.name in states_by_name:
            state = states_by_name[mongration.name]
            logger.info(f"Current mongration state is {state.status.name} (index: {state.index}, phasesRan: {state.phases_ran}, name: {state.name})")
        else:
            state = MongrationState(args.index or len(states), mongration_path.stem, MongrationStatus.ABSENT)
            logger.info("Mongration state isn't present in the database")
        # Set the migration status based on the user's input
        status_to_set = MongrationStatus.by_name(args.status)
        collection = client.get_database("mongrations").get_collection("state")
        if status_to_set == MongrationStatus.WORK_IN_PROGRESS:
            await state.work_in_progress(collection)
        elif status_to_set == MongrationStatus.COMPLETED:
            await state.completed(collection)
        elif status_to_set == MongrationStatus.FAILED:
            await state.failed(collection)

        logger.warning(f'Migration {mongration.name} status forcefully set to {status_to_set.name}')

    def _check_database_state_health(self, states):
        for i, state in enumerate(states[:-1]):
            next = states[i + 1]
            if next.status == MongrationStatus.COMPLETED and state.status != MongrationStatus.COMPLETED:
                logger.error(
                    f"Mongration {state.name} is not yet completed, but next mongration {next.name} is completed, this should not happen. Expected migration order is:"
                )
                msg = ", ".join([f"#{i}: {state.name} ({state.status.name})" for i, state in enumerate(states[:-1])])
                logger.error(msg)
                return False
        return True

    def _list_pending_mongrations(self, states, mongrations):
        # Convert the states list to a dictionary for efficient lookups
        states_dict = {state.name: state for state in states}
        pending_mongrations = []

        for mongration in mongrations:
            # Check if the mongration is either not present in the states or is not completed
            is_pending = mongration.name not in states_dict or states_dict[
                mongration.name].status != MongrationStatus.COMPLETED
            if mongration.is_stateless() or is_pending:
                pending_mongrations.append(mongration)

        return pending_mongrations

    def load_mongration_at_path(self, path: Path) -> Optional[Mongration]:
        try:
            logger.debug(f"Loading mongration from path: {path}")
            script = load_mongration_script(path)
            if script is None:
                logger.error(f"Unable to load mongration at {path}")
                return None
            mongration = load_mongration_from_script(path.stem, script)
            logger.debug(f"Successfully loaded mongration: {mongration.name}")
            return mongration
        except Exception as e:
            logger.exception(f"Caught an exception while trying to load mongration {path}")
            raise Exception(f"Caught an exception while trying to load mongration {path}") from e

    async def _main(self, args, engine):
        url = args.url
        mongration_script = args.mongration
        mongrations_dir = args.mongrations_dir
        paths = []
        if mongration_script is not None:
            mongration_path = Path(mongration_script)
            if not mongration_path.exists() or not mongration_path.is_file():
                logger.error(f"The specified mongration script does not exist: {mongration_script}")
                return
            logger.debug(f"Adding mongration script: {mongration_script}")
            paths.append(mongration_path)

        if mongrations_dir is not None:
            logger.debug(f"Scanning mongrations directory: {mongrations_dir}")
            for dirpath, dirnames, filenames in os.walk(mongrations_dir):
                if '__pycache__' in dirpath.split(os.sep):
                    continue  # Skip this directory
                for file in filenames:
                    paths.append(Path(os.path.join(dirpath, file)))

        mongrations = []
        for path in paths:
            mongration = self.load_mongration_at_path(path)
            if mongration is None:
                continue
            mongrations.append(mongration)

        mongrations.sort(key=lambda mon: mon.name)
        logger.info(f"Total of {len(mongrations)} mongrations loaded")
        if len(mongrations) == 0:
            logger.warning("No mongrations found to execute")
            return
        logger.info(f"Connecting to mongodb at {url}...")
        client = AsyncIOMotorClient(url)
        logger.info("Successfully connected to mongodb")
        state_collection = client.get_database("mongrations").get_collection("state")

        states = await self._fetch_status(state_collection)
        states.sort(key=lambda state: state.index)

        states_by_name: dict[str, MongrationState] = {state.name: state for state in states}
        if not self._check_database_state_health(states):
            logger.error("Database state is not healthy. Aborting.")
            return

        pending_execution = self._list_pending_mongrations(states, mongrations)

        if len(pending_execution) == 0:
            logger.info("All mongrations are up to date.")
            return
        logger.info(f"{len(pending_execution)} mongrations need to be run:")

        for i, mongration in enumerate(pending_execution):
            logger.info(f"#{i}: {mongration.name}, {len(mongration.phases())} phases:")
            for phase in mongration.phases():
                logger.info(f"  * {phase.name()}")

        if args.dry_run:
            logger.info("Dry run specified. Stopping here.")
            return

        os.makedirs("graphs", exist_ok=True)
        for mongration in pending_execution:
            name = mongration.name
            existing = states_by_name.get(name, None)
            if existing is not None:
                index = existing.index
            else:
                index = len(states)
            graph = build_dependency_graph(mongration.phases())

            state = states_by_name.get(
                name,
                MongrationState(index, name, MongrationStatus.ABSENT)
            )
            states.append(state)
            logger.info(f"Running mongration {name}...")
            canvas = graph.to_canvas(circle_radius=10, padding=5, name_selector=lambda phase: phase.name())

            logger.debug(f"Dependency graph for {name}:\n{canvas}")

            with open(f"graphs/{name}.graph.txt", "w") as f:
                f.writelines(str(canvas))
            if mongration.is_stateful():
                await state.work_in_progress(state_collection)
            try:
                if mongration.is_stateful():
                    for phase in mongration.phases():
                        current_phase = phase
                        phase.on_completed(
                            lambda num_docs_iterated: state.notify_phase_completed(
                                state_collection,
                                phase,
                                num_docs_iterated
                            )
                        )
                await engine.invoke(
                    client,
                    mongration,
                    graph,
                )
            except Exception as e:
                if mongration.is_stateful():
                    await state.failed(state_collection)
                logger.exception(f"An exception occurred while running mongration {mongration.name}, phase {current_phase.name()}")
                raise Exception(f"An exception occoured while running mongration {mongration.name}, phase {current_phase.name()}") from e
                
            if mongration.is_stateful():
                await state.completed(state_collection)
            logger.info(f"Successfully completed mongration {name}")
