import logging
from mongrations.io.source import CollectionSource
from mongrations.phase import Phase

logger = logging.getLogger(__name__)


class Mongration:
    def __init__(self, name: str):
        logger.debug(f"Initializing mongration: {name}")
        self.name = name
        self._phases = list[Phase]()
        self._stateless = False

    def phase(self, name):
        logger.debug(f"Creating phase '{name}' for mongration {self.name}")
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
        logger.debug(f"Marking mongration {self.name} as stateless")
        self._stateless = True

    def convert_to_uuid_phase(
        self, 
        database: str, 
        collection: str, 
        field: str = "_id", 
        keep_legacy: bool = False,
        convertion_batch_size=256
    ):
        logger.info(f"Setting up UUID conversion for {database}.{collection}, field: {field}, keep_legacy: {keep_legacy}")
        convert_phase = self.phase(f"Convert {database}.{collection} field {field} to UUID")
        convert_phase.from_collection(database, collection)
        tmp_collection_name = f"tmp-{collection}-uuid-conversion-{field}"
        convert_phase.use_aggregation([
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
                    "_id": {
                        "$function": {
                            "body": """
                            function (id) { 
                              try {
                                return UUID(id);
                              } catch (exception)  {
                                print("Exception caught with _id: " + id + " - Error: " + e.message);
                              }
                            }
                            """,
                            "args": [f"${field}"],
                            "lang": "js"
                        }
                    }
                }
            },
        ]
        )
        convert_phase.into_collection("mongrations", tmp_collection_name)
        copy_phase = self.phase(f"Copy existing {database}.{collection} fields {field} of type UUID")
        copy_phase.from_collection(database, collection)
        copy_phase.use_aggregation(
            [
            {
                "$match": {
                    field: {
                        "$type": "binData"
                    }
                }
            },
        ]
        )
        copy_phase.into_collection("mongrations", tmp_collection_name)
        overwrite_phase = self.phase("Overwrite collection")
        overwrite_phase.wait_for_phase(convert_phase)
        overwrite_phase.wait_for_phase(copy_phase)
        overwrite_phase.from_collection("mongrations", tmp_collection_name)

        if keep_legacy:
            rename_old_collection = self.phase("Rename old collection")
            rename_old_collection.wait_for_phase(convert_phase)
            rename_old_collection.wait_for_phase(copy_phase)
            rename_old_collection.from_collection(database, collection)
            rename_old_collection.rename_collection(f"{collection}__pre_uuid_conversion")
            overwrite_phase.wait_for_phase(rename_old_collection)
        overwrite_phase.use_aggregation(
            []
        )
        overwrite_phase.into_collection(database, f"{collection}_new")
        return convert_phase
