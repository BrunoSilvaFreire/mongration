import os
import traceback
from pathlib import Path
from typing import Optional

from mongrations.mongration import Mongration
from motor.motor_asyncio import AsyncIOMotorClient

from mongrations.engine.asyncio_engine import AsyncIOEngine
from mongrations.loading import load_mongration_script, load_mongration_from_script, build_dependency_graph
from mongrations.plan import MongrationStatus, MongrationState


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
        print(f"Connecting to mongodb...")
        client = AsyncIOMotorClient(url)
        print(f"Connected!")
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
            print(f"Current mongration state is {state.status} (index: {state.index}, phasesRan: {state.phases_ran}, name: {state.name})")
        else:
            state = MongrationState(args.index or len(states), mongration_path.stem, MongrationStatus.ABSENT)
            print("Mongration state isn't present in the database")
        # Set the migration status based on the user's input
        status_to_set = MongrationStatus.by_name(args.status)
        collection = client.get_database("mongrations").get_collection("state")
        if status_to_set == MongrationStatus.WORK_IN_PROGRESS:
            await state.work_in_progress(collection)
        elif status_to_set == MongrationStatus.COMPLETED:
            await state.completed(collection)
        elif status_to_set == MongrationStatus.FAILED:
            await state.failed(collection)

        print(f'Migration {mongration.name} status forcefully set to {status_to_set.name}')

    def _check_database_state_health(self, states):
        for i, state in enumerate(states[:-1]):
            next = states[i + 1]
            if next.status == MongrationStatus.COMPLETED and state.status != MongrationStatus.COMPLETED:
                print(
                    f"Mongration {state.name} is not yet completed, but next mongration {next.name} is completed, this should not happen. Expected migration order is:"
                )
                msg = ", ".join([f"#{i}: {state.name} ({state.status})" for i, state in enumerate(states[:-1])])
                print(msg)
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
            script = load_mongration_script(path)
            if script is None:
                print(f"Unable to load mongration at {path}")
                return None
            return load_mongration_from_script(path.stem, script)
        except Exception as e:
            raise Exception(f"Caught an exception while trying to load mongration {path}") from e

    async def _main(self, args, engine):
        url = args.url
        mongration_script = args.mongration
        mongrations_dir = args.mongrations_dir
        paths = []
        if mongration_script is not None:
            mongration_path = Path(mongration_script)
            if not mongration_path.exists() or not mongration_path.is_file():
                print(f"The specified mongration script does not exist: {mongration_script}")
                return
            paths.append(mongration_path)

        if mongrations_dir is not None:
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
        print(f"Total of {len(mongrations)} mongrations.")
        if len(mongrations) == 0:
            return
        print(f"Connecting to mongodb...")
        client = AsyncIOMotorClient(url)
        print(f"Connected!")
        state_collection = client.get_database("mongrations").get_collection("state")

        states = await self._fetch_status(state_collection)
        states.sort(key=lambda state: state.index)

        states_by_name: dict[str, MongrationState] = {state.name: state for state in states}
        if not self._check_database_state_health(states):
            print("Database state is not healthy. Aborting.")
            return

        pending_execution = self._list_pending_mongrations(states, mongrations)

        if len(pending_execution) == 0:
            print("All mongrations are up to date.")
            return
        print(f"{len(pending_execution)} mongrations need to be run:")

        for i, mongration in enumerate(pending_execution):
            print(f"#{i}: {mongration.name}, {len(mongration.phases())} phases:")
            for phase in mongration.phases():
                print(f"* {phase.name()}")

        if args.dry_run:
            print("Dry run specified. Stopping here.")
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
            print(f"Running mongrations {name}...")
            canvas = graph.to_canvas(circle_radius=10, padding=5, name_selector=lambda phase: phase.name())

            print(canvas)

            with open(f"graphs/{name}.graph.txt", "w") as f:
                f.writelines(str(canvas))
            if mongration.is_stateful():
                await state.work_in_progress(state_collection)
            try:
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
                raise Exception(f"An exception occoured while running mongration {mongration.name}, phase {current_phase.name()}") from e
                
            if mongration.is_stateful():
                await state.completed(state_collection)
