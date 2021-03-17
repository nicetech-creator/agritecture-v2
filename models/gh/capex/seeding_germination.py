from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math
import timeit

class Gh_Seeding_Germination_Capex:

    @staticmethod
    def info():
        return {
            "name": "gh_seeding_germination_capex",
            "description": "Seeding and germination system sizing for GH Capex"
        }

    @staticmethod
    def input_type():
        return {
            "main bedspace": {
                "description": "Main bedspace area (sqft)",
                "type": "decimal"
            },
            "bedspace seeding": {
                "description": "Bedspace area reserved for seeding (sqft)",
                "type": "decimal"
            },
            "seeding racks cost density": {
                "description": "Cost of seeding racks per area ($ / sqft)",
                "default": 5.9,
                "type": "decimal"
            },
            "germination trays cost density": {
                "description": "Cost of germination trays per area ($ / sqft)",
                "default": 7.5,
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
            "line items": {
                "type": "list",
                "description": "Line item costs",
                "element type": {
                    "description": {
                        "type": "text",
                        "description": "Line item description"
                    },
                    "cost": {
                        "description": "Line item expenses cost ($)",
                        "digits": 2,
                        "type": "decimal"
                    }
                }
            },
            "cost": {
                "description": "Total cost ($)",
                "digits": 2,
                "type": "decimal"
            },
            "depreciation": {
                "description": "Depreciation cost per year ($ / y)",
                "digits": 2,
                "type": "decimal"
            },
            "description": {
                "description": "Equipment description",
                "type": "text"
            },
            "number of units": {
                "description": "Number of units required for seeding",
                "type": "integer"
            },
            "automation level": {
                "description": "Seeding automation level code",
                "type": "text"
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
        inputs = parse_inputs(Gh_Seeding_Germination_Capex.input_type(), args)

        output_type = Gh_Seeding_Germination_Capex.output_type()
        result['sql_runtime'] = 0
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        query_inputs = {"bedspace": int(inputs["main bedspace"])}
        s_start = timeit.default_timer()
        seeding_row = fetch_one(
            "SELECT description, price, automation FROM seeding_lookup where max_bedspace >= %(bedspace)s or max_bedspace is null order by price limit 1",
            query_inputs)
        result['sql_runtime'] += timeit.default_timer() - s_start
        if seeding_row is None:
            raise ValueError(
                "No elements found for inputs: " +
                str(query_inputs))

        result['line items'] = [{
            'description': 'Bread Racks',
            'cost': inputs['bedspace seeding'] * inputs['seeding racks cost density']
        }, {
            'description': 'Germination Trays',
            'cost': inputs['bedspace seeding'] * inputs['germination trays cost density']
        }, {
            'description': seeding_row['description'],
            'cost': seeding_row['price']
        }, {
            'description': 'Processing Tables',
            'cost': 2500
        }]

        set_if_unset(
            result, "cost", sum(
                line["cost"] for line in result["line items"]))
        set_if_unset(result, "depreciation", result["cost"] / inputs["depreciation duration"])
        set_if_unset(result, "description", seeding_row["description"])
        set_if_unset(
            result,
            "number of units",
            0 if seeding_row["automation"] == 'N' else 1)
        set_if_unset(result, "automation level", seeding_row["automation"])

        result['runtime'] = timeit.default_timer() - start
        if quantize_output:
            quantize_outputs(output_type, result)

        return result