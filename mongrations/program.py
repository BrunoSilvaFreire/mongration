import os
import traceback
from pathlib import Path

from motor.motor_asyncio import AsyncIOMotorClient

from mongrations.engine.asyncio_engine import AsyncIOEngine
from mongrations.loading import load_mongration_script, load_mongration, build_dependency_graph
from mongrations.plan import MongrationStatus, MongrationState


class MongrationProgram:

    async def _fetch_status(self, state_collection):
        states = list()

        async for doc in state_collection.find():
            states.append(MongrationState(
                index=doc["_id"],
                name=doc['name'],
                status=MongrationStatus.by_name(doc['status'])
            ))
        states.sort(key=lambda state: state.index, reverse=True)
        return states

    def run(self, args):

        engine = AsyncIOEngine()
        engine.start(self._main(args, engine))

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

    async def _main(self, args, engine):
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
            try:
                script = load_mongration_script(path)
                if script is None:
                    print(f"Unable to load mongration at {path}")
                    return
                mongration_name = load_mongration(path.stem, script)
                instance = mongration_name
                mongrations.append(instance)
            except Exception as e:
                new_exception = Exception(f"Caught an exception while trying to load mongration {path}")
                new_exception.__cause__ = e
                traceback.print_exception(new_exception)  # This prints the stack trace of the exception `e`

        mongrations.sort(key=lambda mon: mon.name)
        print(f"Total of {len(mongrations)} mongrations.")
        if len(mongrations) == 0:
            return
        print(f"Connecting to mongodb...")
        client = AsyncIOMotorClient("mongodb://root:letmein@localhost:27017")
        print(f"Connected!")
        state_collection = client.get_database("mongrations").get_collection("state")

        states = await self._fetch_status(state_collection)
        states.sort(key=lambda state: state.index)

        states_by_name = {state.name: state for state in states}
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
            index = states_by_name.get(name, None)
            if index is None:
                index = len(states)
            graph = build_dependency_graph(mongration.phases())

            state = states_by_name.get(
                name,
                MongrationState(index, name, MongrationStatus.ABSENT)
            )
            print(f"Running mongrations {name}...")
            canvas = graph.to_canvas(circle_radius=10, padding=5, name_selector=lambda phase: phase.name())

            print(canvas)

            with open(f"graphs/{name}.graph.txt", "w") as f:
                f.writelines(str(canvas))
            if mongration.is_stateful():
                await state.work_in_progress(state_collection)
            try:
                for phase in mongration.phases():
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
                raise e
            if mongration.is_stateful():
                await state.completed(state_collection)
