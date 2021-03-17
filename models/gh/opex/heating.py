from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math
import timeit

class Gh_Heating_Opex:

    @staticmethod
    def info():
        return {
            "name": "gh_heating_opex",
            "description": "Heating opex sizing for GH"
        }

    @staticmethod
    def input_type():
        return {
            "heating required": {
                "description": "Heating energy required per year (kWh / y)",
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
            "natural gas required": {
                "description": "Amount of natural gas required (cuft)",
                "digits": 1,
                "type": "decimal"
            },
            "runtime": {
                "digits": 4,
                "default": 0,
                "description": "Time taken for this module run",
                "type": "decimal"
            },
            "sql_runtime": {
                "digits": 4,
                "default": 0,
                "description": "Time taken for sql interation in this module",
                "type": "decimal"
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def compute(args, quantize_output=False):
        start = timeit.default_timer()
        result = dict()
        inputs = parse_inputs(Gh_Heating_Opex.input_type(), args)

        output_type = Gh_Heating_Opex.output_type()
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        set_if_unset(
            result,
            "natural gas required",
            inputs["heating required"] /
            0.2931)

        result['runtime'] = timeit.default_timer() - start
        if quantize_output:
            quantize_outputs(output_type, result)

        return result
