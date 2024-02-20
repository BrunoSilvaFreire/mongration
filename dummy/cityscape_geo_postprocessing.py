import asyncio
import time

from shapely.geometry import shape, mapping
from shapely.ops import unary_union
from mongrations.mongration import Mongration
import yaml


def read_config(config_path):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)


def correct_polygon(doc):
    geom = shape(doc['geometry'])
    if geom.is_valid:
        corrected_geom = unary_union(geom)
        doc['geometry'] = mapping(corrected_geom)
        doc['original_id'] = doc['_id']
    return doc


def remove_null_properties(doc: dict):
    properties = doc['properties']
    new_properties = dict()
    for key in properties:
        value = properties[key]
        if value is None:
            new_properties[key] = value
    if len(new_properties) == 0:
        doc.pop('properties')
    else:
        doc['properties'] = new_properties
    return doc


def identity(doc):
    return doc


def mongration(mongration: Mongration):
    remote_intersecting_geo_objs = mongration.phase("Remove intersecting GeoJSON objects")
    remote_intersecting_geo_objs.from_collection("lots", "geojson")
    remote_intersecting_geo_objs.use_python(correct_polygon)

    clean_up = mongration.phase("Cleanup Properties")
    clean_up.from_phase(remote_intersecting_geo_objs)
    clean_up.use_python(remove_null_properties)

    associate_with_address = mongration.phase("Associate GeoJSON with address")
    associate_with_address.from_phase(clean_up)
    associate_with_address.use_aggregation(
        "lots",
        "geometry",
        [
            {
                "$match": {
                    "_id": {
                        "$exists": True
                    }
                }
            },
            {
                "$set": {
                    "IT_WORKS": True
                }
            }
        ]
    )
    associate_with_address.into_collection("lots", "geometry")
