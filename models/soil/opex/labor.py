from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math


class Soil_Labor:

    @staticmethod
    def info():
        return {
            "name": "soil_labor",
            "description": "Labor schedule selector for Soil"
        }

    @staticmethod
    def input_type():
        return {
            "bedspace": {
                "description": "Bedspace area that needs to be covered by labor (sqft)",
                "type": "decimal"
            },
            "bedspace per laborer": {
                "description": "Expected bedspace covered by a laborer (sqft)",
                "default": 10000,
                "type": "decimal"
            },
            "fringe cost": {
                "description": "Fringe costs for labor",
                "default": 0.1,
                "type": "decimal"
            },
            "owner is headgrower": {
                "description": "Whether owner counts as headgrower",
                "type": "bool",
                "default": True
            },
            "headgrower salary": {
                "description": "Salary for headgrower ($ / y)",
                "default": 60000,
                "type": "decimal"
            },
            "labor wages": {
                "description": "Salary for operators ($ / h)",
                "default": 15.73,
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
            "labor": {
                "type": "list",
                "description": "List of labor positions required",
                "element type": {
                    "role": {
                        "description": "Role description",
                        "type": "text",
                        "primary key": True
                    },
                    "bedspace per role": {
                        "description": "Amount of bedspace covered by role",
                        "digits": 0,
                        "type": "decimal"
                    },
                    "number of staff": {
                        "description": "Number of positions of this type required",
                        "digits": 2,
                        "type": "decimal"
                    },
                    "annual cost per role": {
                        "description": "Cost spent per year in each position of this role ($ / staff / y)",
                        "digits": 2,
                        "type": "decimal"
                    },
                    "annual cost": {
                        "description": "Total annual cost spent in this role ($ / year)",
                        "digits": 2,
                        "type": "decimal"
                    }
                }
            },
            "total staff": {
                "type": "integer",
                "description": "Total staff on labor requirements"
            },
            "total cost": {
                "digits": 2,
                "description": "Total annual cost on labor requirements ($)",
                "type": "decimal"
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def compute(args, quantize_output=False):
        result = dict()
        inputs = parse_inputs(Soil_Labor.input_type(), args)

        output_type = Soil_Labor.output_type()
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        bedspace_per_laborer = inputs["bedspace per laborer"]
        fringe_multiplier = (1 + inputs["fringe cost"])
        if inputs['owner is headgrower']:
            result['labor'] = [{
                'role': 'Head Grower',
                'bedspace per role': 10 * bedspace_per_laborer,
                'number of staff': int(math.floor(inputs['bedspace'] / (10 * bedspace_per_laborer))),
                'annual cost per role': inputs['headgrower salary'] * fringe_multiplier
            }, {
                'role': 'Operator / asst',
                'bedspace per role': bedspace_per_laborer,
                'number of staff': inputs['bedspace'] / bedspace_per_laborer,
                'annual cost per role': inputs['labor wages'] * 40 * 52 * fringe_multiplier
            }]
        else:
            result['labor'] = [{
                'role': 'Head Grower',
                'bedspace per role': 10 * bedspace_per_laborer,
                'number of staff': 1 + int(math.floor(inputs['bedspace'] / (10 * bedspace_per_laborer))),
                'annual cost per role': inputs['headgrower salary'] * fringe_multiplier
            }, {
                'role': 'Operator / asst',
                'bedspace per role': bedspace_per_laborer,
                'number of staff': inputs['bedspace'] / bedspace_per_laborer,
                'annual cost per role': inputs['labor wages'] * 40 * 52 * fringe_multiplier
            }]
        for d in result['labor']:
            d['annual cost'] = d['annual cost per role'] * d['number of staff']

        set_if_unset(
            result, "total staff", sum(
                line["number of staff"] for line in result["labor"]))
        set_if_unset(
            result, "total cost", sum(
                line["annual cost"] for line in result["labor"]))

        if quantize_output:
            quantize_outputs(output_type, result)

        return result