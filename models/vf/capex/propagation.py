from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math


class Vf_Propagation_Capex:

    @staticmethod
    def info():
        return {
            "name": "vf_propagation_capex",
            "description": "Propagation for VF Capex"
        }

    @staticmethod
    def input_type():
        return {
            "depreciation duration": {
                "description": "Duration of item depreciation",
                "default": 20,
                "type": "decimal"
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def input_values(input_name):
        raise ValueError("Unexpected input: " + str(input))

    @staticmethod
    def output_type():
        return {
            "dosing and ph control cost": {
                "description": "Costs spent on dosing and pH control ($)",
                "digits": 2,
                "type": "decimal"
            },
            "cost": {
                "description": "Total cost for propagation ($)",
                "digits": 2,
                "type": "decimal"
            },
            "depreciation": {
                "description": "Depreciation cost per year ($ / y)",
                "digits": 2,
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
        result = dict()
        inputs = parse_inputs(Vf_Propagation_Capex.input_type(), args)

        output_type = Vf_Propagation_Capex.output_type()
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        set_if_unset(result, "dosing and ph control cost", 1500)
        set_if_unset(result, "cost", result["dosing and ph control cost"])
        set_if_unset(result, "depreciation", result["cost"] / inputs['depreciation duration'])

        if quantize_output:
            quantize_outputs(output_type, result)

        return result