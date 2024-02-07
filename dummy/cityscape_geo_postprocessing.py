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


def mongration(mongration: Mongration):
    remote_intersecting_geo_objs = mongration.phase("Remove intersecting GeoJSON objects")
    remote_intersecting_geo_objs.from_collection("lots", "geojson")
    remote_intersecting_geo_objs.use_python(correct_polygon)
    remote_intersecting_geo_objs.into_temporary("lots")

    associate_with_address = mongration.phase("Associate GeoJSON with address")
    associate_with_address.from_phase(remote_intersecting_geo_objs)
    associate_with_address.into_collection("lots", "geometry")
