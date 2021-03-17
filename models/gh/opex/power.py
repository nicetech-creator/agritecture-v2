from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math


class Gh_Power:

    @staticmethod
    def info():
        return {
            "name": "gh_power",
            "description": "Power use breakdown for GH"
        }

    @staticmethod
    def input_type():
        return {
            "bedspace": {
                "description": "Bedspace area requiring power (sqft)",
                "type": "decimal"
            },
            "electricity cost": {
                "description": "Cost of electricity ($ / kWh)",
                "type": "decimal"
            },
            "lighting power density": {
                "description": "Required lighting power density (kW / sqft)",
                "type": "decimal"
            },
            "ventilation power": {
                "description": "Required ventilation power (kWh / y)",
                "type": "decimal"
            },
            "pumps power density": {
                "description": "Required pumps power density (kW / sqft)",
                "default": 0.0004,
                "type": "decimal"
            },
            "lighting hours per year": {
                "description": "Required hours of lighting per year (h / y)",
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
            "utilities": {
                "type": "list",
                "description": "Utility properties",
                "element type": {
                    "description": {
                        "type": "text",
                        "description": "Utility description"
                    },
                    "power density": {
                        "description": "Required utility power density (kW / sqft)",
                        "digits": 7,
                        "type": "decimal"
                    },
                    "yearly power": {
                        "description": "Utility power required per year (kWh / y)",
                        "digits": 0,
                        "type": "decimal"
                    },
                    "yearly cost": {
                        "description": "Utility cost per year ($ / y)",
                        "digits": 2,
                        "type": "decimal"
                    }
                }
            },
            "total power density": {
                "description": "Total required power density (kW / sqft)",
                "digits": 7,
                "type": "decimal"
            },
            "total power": {
                "description": "Total power required per year (kWh / y)",
                "digits": 0,
                "type": "decimal"
            },
            "total cost": {
                "description": "Total operating costs for power use per year ($ / y)",
                "digits": 2,
                "type": "decimal"
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def compute(args, quantize_output=False):
        result = dict()
        inputs = parse_inputs(Gh_Power.input_type(), args)

        output_type = Gh_Power.output_type()
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        result['utilities'] = [{
            'description': 'Lighting',
            'power density': inputs['lighting power density'],
            'yearly power': inputs['lighting power density'] * inputs['bedspace'] * inputs['lighting hours per year']
        }, {
            'description': 'Ventilation',
            'power density': inputs['ventilation power'] / (24 * 365 * inputs['bedspace']),
            'yearly power': inputs['ventilation power']
        }, {
            'description': 'Pumps',
            'power density': inputs['pumps power density'],
            'yearly power': inputs['pumps power density'] * inputs['bedspace'] * 24 * 365
        }]
        for line in result['utilities']:
            set_if_unset(
                line,
                'yearly cost',
                line["yearly power"] *
                inputs["electricity cost"])

        set_if_unset(result, "total power density", sum(
            line["power density"] for line in result["utilities"]))
        set_if_unset(
            result, "total power", sum(
                line["yearly power"] for line in result["utilities"]))
        set_if_unset(
            result, "total cost", sum(
                line["yearly cost"] for line in result["utilities"]))

        if quantize_output:
            quantize_outputs(output_type, result)

        return result
