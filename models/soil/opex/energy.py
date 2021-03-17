from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math


class Soil_Energy:

    @staticmethod
    def info():
        return {
            "name": "soil_energy",
            "description": "Energy model for Soil"
        }

    @staticmethod
    def input_type():
        return {
            "cold storage capex": {
                "description": "Capex for Cold Storage ($)",
                "type": "decimal"
            },
            "tools capex": {
                "description": "Capex for Tools ($)",
                "type": "decimal"
            },
            "transportation capex": {
                "description": "Capex for Transportation ($)",
                "type": "decimal"
            },
            "harvest capex": {
                "description": "Capex for Harvesting, washing and packaging ($)",
                "type": "decimal"
            },
            "opex fraction": {
                "description": "Estimated fraction of Capex as Opex (%)",
                "default": 0.02,
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
                "description": "List of energy opex",
                "element type": {
                    "description": {
                        "type": "text",
                        "description": "Line item description"
                    },
                    "cost": {
                        "description": "Line item expenses cost ($ / y)",
                        "digits": 2,
                        "type": "decimal"
                    }
                }
            },
            "total cost": {
                "digits": 2,
                "description": "Total energy opex ($ / y)",
                "type": "decimal"
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def compute(args, quantize_output=False):
        result = dict()
        inputs = parse_inputs(Soil_Energy.input_type(), args)

        output_type = Soil_Energy.output_type()
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        result['line items'] = [{
            'description': 'Cold Storage',
            'cost': inputs['cold storage capex'] * inputs['opex fraction']
        }, {
            'description': 'Tools',
            'cost': inputs['tools capex'] * inputs['opex fraction']
        }, {
            'description': 'Transportation',
            'cost': inputs['transportation capex'] * inputs['opex fraction']
        }, {
            'description': 'Harvesting, washing & packaging equipment',
            'cost': inputs['harvest capex'] * inputs['opex fraction']
        }]

        set_if_unset(
            result, "total cost", sum(
                line["cost"] for line in result["line items"]))

        if quantize_output:
            quantize_outputs(output_type, result)

        return result
