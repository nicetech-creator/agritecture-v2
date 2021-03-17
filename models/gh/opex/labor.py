from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset, arr2dic
import math
import timeit

class Gh_Labor:

    @staticmethod
    def info():
        return {
            "name": "gh_labor",
            "description": "Labor schedule selector for GH"
        }

    @staticmethod
    def input_type():
        return {
            "bedspace": {
                "description": "Bedspace area that needs to be covered by labor (sqft)",
                "type": "decimal"
            },
            "automation level": {
                "description": "Desired level of automation",
                "type": "text"
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
            },
            "advanced":{
                "type": "list",
                "description": "Extra info used for advanced reporting in FE",
                "element type": {
                    "description": {
                        "type": "text",
                        "description": "Line item description"
                    },
                    "value": {
                        "description": "advanced info value",
                        "type": "any"
                    }
                }
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
                    "advanced name": {
                        "description": "Name of advanced option",
                        "type": "text"
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
        inputs = parse_inputs(Gh_Labor.input_type(), args)
        advanced_inputs = arr2dic(inputs["advanced"])
        output_type = Gh_Labor.output_type()
        result['sql_runtime'] = 0
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        query_inputs = {"automation": inputs["automation level"]}
        s_start = timeit.default_timer()
        bedspace_row = fetch_one(
            "SELECT bedspace_per_laborer FROM gh_labor WHERE automation=%(automation)s",
            query_inputs)
        result['sql_runtime'] += timeit.default_timer() - s_start
        if bedspace_row is None:
            raise ValueError(
                "No elements found for inputs: " +
                str(query_inputs))

        bedspace_per_laborer = bedspace_row["bedspace_per_laborer"]
        fringe_multiplier = (1 + inputs["fringe cost"])
        if inputs['owner is headgrower']:
            result['labor'] = [{
                'role': 'Head Grower',
                'advanced name': 'headgrower number',
                'bedspace per role': 10 * bedspace_per_laborer,
                'number of staff': advanced_inputs["headgrower number"] if "headgrower number" in advanced_inputs else max(inputs['bedspace'] / (10 * bedspace_per_laborer) - 1, 0),
                'annual cost per role': inputs['headgrower salary'] * fringe_multiplier
            }, {
                'role': 'Operator / asst',
                'advanced name': 'stuff number',
                'bedspace per role': bedspace_per_laborer,
                'number of staff': advanced_inputs["stuff number"] if "stuff number" in advanced_inputs else inputs['bedspace'] / bedspace_per_laborer,
                'annual cost per role': inputs['labor wages'] * 40 * 52 * fringe_multiplier
            }]
        else:
            result['labor'] = [{
                'role': 'Head Grower',
                'advanced name': 'headgrower number',
                'bedspace per role': 10 * bedspace_per_laborer,
                'number of staff': advanced_inputs["headgrower number"] if "headgrower number" in advanced_inputs else max(inputs['bedspace'] / (10 * bedspace_per_laborer), 1),
                'annual cost per role': inputs['headgrower salary'] * fringe_multiplier
            }, {
                'role': 'Operator / asst',
                'advanced name': 'stuff number',
                'bedspace per role': bedspace_per_laborer,
                'number of staff': advanced_inputs["stuff number"] if "stuff number" in advanced_inputs else inputs['bedspace'] / bedspace_per_laborer,
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

        result['runtime'] = timeit.default_timer() - start
        if quantize_output:
            quantize_outputs(output_type, result)

        return result
