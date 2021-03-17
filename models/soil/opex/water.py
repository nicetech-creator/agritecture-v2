from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math


class Soil_Water:

    @staticmethod
    def info():
        return {
            "name": "soil_water",
            "description": "Water model for Soil"
        }

    @staticmethod
    def input_type():
        return {
            "water used": {
                "description": "Amount of water used yearly (gallons / y)",
                "type": "decimal"
            },
            "water cost": {
                "description": "Cost of water ($ / gallon)",
                "default": 0.002,
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
            "total cost": {
                "description": "Total water cost ($ / y)",
                "digits": 2,
                "type": "decimal"
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def compute(args, quantize_output=False):
        result = dict()
        inputs = parse_inputs(Soil_Water.input_type(), args)

        output_type = Soil_Water.output_type()
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        set_if_unset(
            result,
            "total cost",
            inputs["water used"] *
            inputs["water cost"])

        if quantize_output:
            quantize_outputs(output_type, result)

        return result
