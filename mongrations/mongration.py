from mongrations.phase import Phase


class Mongration:
    def __init__(self, name: str):
        self.name = name
        self._phases = list[Phase]()
        self._stateless = False

    def phase(self, name):
        ph = Phase(name)
        self._phases.append(ph)
        return ph

    def phases(self) -> list[Phase]:
        return self._phases

    def is_stateless(self):
        return self._stateless

    def is_stateful(self):
        return not self._stateless

    def mark_stateless(self):
        self._stateless = True

    def convert_to_uuid_phase(self, database: str, collection: str, field: str = "_id", keep_legacy: bool = True):
        phase = self.phase(f"Convert {field} to UUID")
        phase.from_collection(database, collection)
        tmp_collection_name = f"tmp-{collection}-uuid-conversion-{field}"
        phase.use_aggregation([
            {
                "$match": {
                    field: {
                        "$not": {
                            "$type": "binData"
                        }
                    }
                }
            },
            {
                "$set": {
                    field: {
                        "$function": {
                            "body": "function (id) { return UUID(id); }",
                            "args": [f"${field}"],
                            "lang": "js"
                        }
                    }
                }
            },
            {
                "$out": {
                    "into": tmp_collection_name,
                    "whenMatched": "replace",
                    "whenNotMatched": "insert"
                }
            }
        ])
        mid_phase: Phase
        if keep_legacy:
            mid_phase = self.phase("Move legacy collection")
            mid_phase.wait_for_phase(phase)
            mid_phase.from_collection(database, collection)
            mid_phase.rename_collection(f"{collection}__legacy")
        else:
            mid_phase = self.phase("Move legacy collection")
            mid_phase.wait_for_phase(phase)
            mid_phase.from_collection(database, collection)
            mid_phase.rename_collection(f"{collection}__legacy")
        effetuate_new_collection = self.phase("Rename collections")
        effetuate_new_collection.wait_for_phase(move_legacy_collection)
        move_legacy_collection.from_collection(database, tmp_collection_name)
        effetuate_new_collection.rename_collection(collection)

        return phase
