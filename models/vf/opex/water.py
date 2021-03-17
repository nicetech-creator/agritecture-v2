from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math


class Vf_Water:

    @staticmethod
    def info():
        return {
            "name": "vf_water",
            "description": "Water model for VF"
        }

    @staticmethod
    def input_type():
        return {
            "water transpired": {
                "description": "Total water transpired yearly (l)",
                "type": "decimal"
            },
            "employees needed": {
                "description": "Number of employees needed",
                "type": "decimal"
            },
            "total area per week": {
                "description": "Total cultivated area per week (sqft / week)",
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
            "line items": {
                "type": "list",
                "description": "Line item usage",
                "element type": {
                    "description": {
                        "type": "text",
                        "description": "Line item description"
                    },
                    "water usage": {
                        "description": "Water usage (gallons / y)",
                        "digits": 0,
                        "type": "decimal"
                    }
                }
            },
            "total water": {
                "digits": 0,
                "description": "Total water usage (gallons / y)",
                "type": "decimal"
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def compute(args, quantize_output=False):
        result = dict()
        inputs = parse_inputs(Vf_Water.input_type(), args)

        output_type = Vf_Water.output_type()
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        result['line items'] = [{
            'description': 'Plant biomass',
            'water usage': inputs['water transpired'] * 0.264 * 1.7
        }, {
            'description': 'Employees',
            'water usage': (inputs['employees needed'] + 2) * 40 * 365
        }, {
            'description': 'Cleaning',
            'water usage': inputs['total area per week'] * 52 * 10
        }]

        set_if_unset(
            result, "total water", sum(
                line["water usage"] for line in result["line items"]))

        if quantize_output:
            quantize_outputs(output_type, result)

        return result
