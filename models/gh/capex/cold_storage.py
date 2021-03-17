from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math
import timeit

class Gh_Cold_Storage_Capex:

    @staticmethod
    def info():
        return {
            "name": "gh_cold_storage_capex",
            "description": "Cold storage system sizing for GH"
        }

    @staticmethod
    def input_type():
        return {
            "bedspace": {
                "description": "Total bedspace area (sqft)",
                "type": "decimal"
            },
            "cold storage area fraction": {
                "description": "Fraction of area reserved for cold storage (%)",
                "default": 0.015,
                "type": "decimal"
            },
            "cold storage area cost": {
                "description": "Cost per area of cold storage ($ / sqft)",
                "default": 43,
                "type": "decimal"
            },
            "depreciation duration": {
                "description": "Duration of item depreciation",
                "default": 20,
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
            "cold storage bedspace": {
                "description": "Area reserved for cold storage (sqft)",
                "digits": 0,
                "type": "decimal"
            },
            "cost": {
                "description": "Total cost for cold storage ($)",
                "digits": 2,
                "type": "decimal"
            },
            "depreciation": {
                "description": "Depreciation cost per year ($ / y)",
                "digits": 2,
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
            },
            "depreciation duration": {
                "description": "Duration of item depreciation",
                "default": 20,
                "type": "decimal"
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def compute(args, quantize_output=False):
        start = timeit.default_timer()
        result = dict()
        inputs = parse_inputs(Gh_Cold_Storage_Capex.input_type(), args)

        output_type = Gh_Cold_Storage_Capex.output_type()
        result['sql_runtime'] = 0
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        set_if_unset(
            result,
            "cold storage bedspace",
            inputs["bedspace"] *
            inputs["cold storage area fraction"])
        set_if_unset(
            result,
            "cost",
            result["cold storage bedspace"] *
            inputs["cold storage area cost"])
        set_if_unset(result, "depreciation", result["cost"] / inputs["depreciation duration"])

        result['runtime'] = timeit.default_timer() - start
        if quantize_output:
            quantize_outputs(output_type, result)

        return result