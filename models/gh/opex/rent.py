from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math


class Gh_Rent:

    @staticmethod
    def info():
        return {
            "name": "gh_rent",
            "description": "Rent sizing for GH"
        }

    @staticmethod
    def input_type():
        return {
            "rent area": {
                "description": "Total area to be rented (sqft)",
                "type": "decimal"
            },
            "rent cost": {
                "description": "Rental cost per area ($ / sqft)",
                "type": "decimal"
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def input_values(input_name):
        raise ValueError("Unexpected input: " + str(input_name))

    @staticmethod
    def output_type():
        return {
            "total rent": {
                "description": "Total rent costs ($)",
                "digits": 2,
                "type": "decimal"
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def compute(args, quantize_output=False):
        result = dict()
        inputs = parse_inputs(Gh_Rent.input_type(), args)

        output_type = Gh_Rent.output_type()
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        set_if_unset(
            result,
            "total rent",
            inputs["rent area"] *
            inputs["rent cost"])

        if quantize_output:
            quantize_outputs(output_type, result)

        return result
