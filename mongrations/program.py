import os
from pathlib import Path

from motor.motor_asyncio import AsyncIOMotorClient

from mongrations.engine.asyncio_engine import AsyncIOEngine
from mongrations.loading import load_mongration_script, load_mongration, build_dependency_graph
from mongrations.plan import MongrationStatus, MongrationState


class MongrationProgram:

    async def _fetch_status(self, client):
        state_collection = client.get_database("mongrations").get_collection("state")
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
                    f"Mongration {state.name} is not yet completed, but next mongration {next.name} is completed, this should not happen.")
                return False
        return True

    def list_pending_mongrations(self, states, mongrations):
        # Convert the states list to a dictionary for efficient lookups
        states_dict = {state.name: state for state in states}
        pending_mongrations = []

        for mongration in mongrations:
            # Check if the mongration is either not present in the states or is not completed
            if mongration.name not in states_dict or states_dict[mongration.name].status != MongrationStatus.COMPLETED:
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
            paths.append(paths)

        if mongrations_dir is not None:
            for dirpath, dirnames, filenames in os.walk(mongrations_dir):
                for file in filenames:
                    paths.append(Path(os.path.join(dirpath, file)))

        mongrations = []
        for path in paths:
            try:
                script = load_mongration_script(path)
                if script is None:
                    continue
                instance = load_mongration(path.stem, script)
                mongrations.append(instance)
            except Exception as e:
                print(e)
                continue
        mongrations.sort(key=lambda mon: mon.name)

        client = AsyncIOMotorClient("mongodb://root:letmein@localhost:27017")
        states = await self._fetch_status(client)

        if not self._check_database_state_health(states):
            print("Database state is not healthy. Aborting.")
            return

        pending_execution = self.list_pending_mongrations(states, mongrations)

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
        for mongration in mongrations:
            graph = build_dependency_graph(mongration.phases())
            print(f"Running mongrations {mongration.name}...")
            canvas = graph.to_canvas(circle_radius=10, padding=5, name_selector=lambda phase: phase.name())

            print(canvas)

            with open(f"graphs/{mongration.name}.graph.txt", "w") as f:
                f.writelines(str(canvas))
            await engine.invoke(client, mongration, graph)
