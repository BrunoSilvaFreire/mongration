import shapely
import yaml
from shapely.geometry import shape, mapping
from shapely.ops import unary_union

from mongrations.misc.documents import deep_set
from mongrations.mongration import Mongration


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
        if value is not None:
            new_properties[key] = value
    if len(new_properties) == 0:
        doc.pop('properties')
    else:
        doc['properties'] = new_properties
    return doc


def compute_area(doc: dict):
    geom = shape(doc['geometry'])
    if geom.is_valid:
        deep_set(doc, 'properties', 'area', shapely.area(geom))
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


    append_area = mongration.phase("Append geometry area to properties")
    append_area.from_phase(clean_up)
    append_area.use_python(compute_area)
    append_area.into_collection("lots", "geometry")
