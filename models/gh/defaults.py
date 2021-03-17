from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math

from models.location.rates import Location_Rates


class Gh_Defaults:

    @staticmethod
    def info():
        return {
            "name": "gh_defaults",
            "description": "Default input values for GH"
        }

    @staticmethod
    def input_type():
        return {
            "location": {
                "type": "text",
                "description": "Text description of the location"
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def input_values(input_name):
        raise ValueError("Unexpected input: " + str(input_name))

    @staticmethod
    def output_type():
        return {
            "latitude": {
                "description": "Geographical latitude",
                "digits": 7,
                "type": "decimal"
            },
            "longitude": {
                "description": "Geographical longitude",
                "digits": 7,
                "type": "decimal"
            },
            "electricity cost": {
                "description": "Cost of electricity ($ / kWh)",
                "digits": 4,
                "type": "decimal"
            },
            "labor wages": {
                "description": "Labor wages ($ / h)",
                "digits": 4,
                "type": "decimal"
            },
            "water cost": {
                "description": "Water cost ($ / gallon)",
                "digits": 7,
                "type": "decimal"
            },
            "gas cost": {
                "description": "Gas cost",
                "digits": 3,
                "type": "decimal"
            },
            "gas cost unit id": {
                "description": "Selected sale unit for gas cost",
                "digits": 0,
                "type": "decimal"
            },
            "tax rate": {
                "description": "Tax rate (%)",
                "digits": 4,
                "type": "decimal"
            },
            "rent cost": {
                "description": "Rent cost per area ($ / sqft month)",
                "digits": 4,
                "type": "decimal"
            },
            "structure type": {
                "type": "integer",
                "description": "Structure type"
            },
            "grow system type": {
                "type": "integer",
                "description": "Selection for grow system type"
            },
            "organic production": {
                "description": "Whether the system will use organic production",
                "type": "bool"
            },
            "supplementary lighting": {
                "description": "Whether the system will include supplementary lighting",
                "type": "bool"
            },
            "heating": {
                "description": "Whether the system will include heating",
                "type": "bool"
            },
            "co2 injection": {
                "description": "Whether the system will include CO2 injection",
                "type": "bool"
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def compute(args, quantize_output=False):
        result = dict()
        inputs = parse_inputs(Gh_Defaults.input_type(), args)

        output_type = Gh_Defaults.output_type()
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        args_location_rates = dict()
        args_location_rates["location"] = inputs["location"]
        result["location_rates"] = Location_Rates.compute(args_location_rates, quantize_output=True)

        set_if_unset(result, "latitude", result["location_rates"]["latitude"])
        set_if_unset(
            result,
            "longitude",
            result["location_rates"]["longitude"])
        set_if_unset(
            result,
            "electricity cost",
            result["location_rates"]["electricity cost"])
        set_if_unset(
            result,
            "labor wages",
            result["location_rates"]["labor wages"])
        set_if_unset(
            result,
            "water cost",
            result["location_rates"]["water cost"])
        set_if_unset(result, "gas cost", result["location_rates"]["gas cost"])
        set_if_unset(
            result,
            "gas cost unit id",
            result["location_rates"]["gas cost unit id"])
        set_if_unset(result, "tax rate", result["location_rates"]["tax rate"])
        set_if_unset(
            result,
            "rent cost",
            result["location_rates"]["rent cost"])
        from annoy import AnnoyIndex
        TREE_FILENAME_GH_DEFAULTS = "files/gh_defaults.ann"
        NUM_FEATURES = 2
        MAX_NEIGHBORS = 1

        def load_trees(tree_filename):
            """Loads the ANN file into the data structure"""
            u = AnnoyIndex(NUM_FEATURES, 'euclidean')
            u.load(tree_filename)
            return u

        def get_point(argument, filename):
            if argument not in globals():
                globals()[argument] = load_trees(filename)
            loaded_trees = globals()[argument]
            latitude = float(result['latitude'])
            longitude = float(result['longitude'])
            nn, distances = loaded_trees.get_nns_by_vector(
                (latitude, longitude), MAX_NEIGHBORS, include_distances=True)
            for i, d in zip(nn, distances):
                v = loaded_trees.get_item_vector(i)
                return v[0], v[1]
        default_latitude, default_longitude = get_point(
            'loaded_trees_gh_defaults', TREE_FILENAME_GH_DEFAULTS)
        sql = "select * from gh_defaults where latitude=%(latitude)s and longitude=%(longitude)s"
        query_inputs = {
            'latitude': default_latitude,
            'longitude': default_longitude}
        row_default = fetch_one(sql, query_inputs)

        set_if_unset(result, "structure type", row_default["structure"])
        set_if_unset(result, "grow system type", 1)
        set_if_unset(result, "organic production", False)
        set_if_unset(
            result,
            "supplementary lighting",
            row_default["lighting"] != 0)
        set_if_unset(
            result,
            "co2 injection",
            row_default["co2_injection"] != 0)
        set_if_unset(result, "heating", row_default["heating"] != 0)

        if quantize_output:
            quantize_outputs(output_type, result)

        return result
