from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math
import timeit

class Vf_Sizing_Seeding:

    @staticmethod
    def info():
        return {
            "name": "vf_sizing_seeding",
            "description": "Seeding area allocation for VF"
        }

    @staticmethod
    def input_type():
        return {
            "annual number of plant sites": {
                "description": "Annual number of plant sites",
                "default": 0,
                "type": "decimal"
            },
            "total substrate req": {
                "description": "Total substrate requirement per year (sqft / y)",
                "default": 0,
                "type": "decimal"
            },
            "seeding loss": {
                "description": "Expected amount of seeding loss (%)",
                "default": 0.1,
                "type": "decimal"
            },
            "seeding plugs per tray": {
                "description": "Number of plugs per tray for seeding",
                "default": 128,
                "type": "decimal"
            },
            "seedlings per area": {
                "description": "Number of seedings per area (1 / sqft)",
                "default": 25,
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
            "seeding annual plug requirement": {
                "description": "Required number of plugs per year",
                "digits": 0,
                "type": "decimal"
            },
            "seeding weekly plug requirement": {
                "description": "Required number of plugs per week",
                "digits": 2,
                "type": "decimal"
            },
            "seeding starter trays per week": {
                "description": "Number of starter trays per week",
                "digits": 0,
                "type": "decimal"
            },
            "seeding bedspace per week non-microgreens": {
                "description": "Bedspace seeded per week, non-microgreens (sqft)",
                "digits": 0,
                "type": "decimal"
            },
            "seeding bedspace needed non-microgreens": {
                "description": "Bedspace needed for seeding, non-microgreens (sqft)",
                "digits": 0,
                "type": "decimal"
            },
            "seeding bedspace needed microgreens": {
                "description": "Bedspace needed for seeding, microgreens (sqft)",
                "digits": 0,
                "type": "decimal"
            },
            "total bedspace needed": {
                "description": "Total bedspace needed (sqft)",
                "digits": 0,
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
        inputs = parse_inputs(Vf_Sizing_Seeding.input_type(), args)

        output_type = Vf_Sizing_Seeding.output_type()
        result['sql_runtime'] = 0
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        set_if_unset(result, "seeding annual plug requirement",
                     inputs["annual number of plant sites"] * (1 + inputs["seeding loss"]))
        set_if_unset(result, "seeding weekly plug requirement",
                     result["seeding annual plug requirement"] / 52)
        set_if_unset(
            result,
            "seeding starter trays per week",
            result["seeding weekly plug requirement"] /
            inputs["seeding plugs per tray"])
        set_if_unset(
            result,
            "seeding bedspace per week non-microgreens",
            result["seeding weekly plug requirement"] /
            inputs["seedlings per area"])
        set_if_unset(
            result,
            "seeding bedspace needed non-microgreens",
            result["seeding bedspace per week non-microgreens"] * 3)
        set_if_unset(result, "seeding bedspace needed microgreens",
                     math.ceil(0.5 * inputs["total substrate req"]/52))
        set_if_unset(
            result,
            "total bedspace needed",
            result["seeding bedspace needed non-microgreens"] +
            result["seeding bedspace needed microgreens"])

        result['runtime'] = timeit.default_timer() - start
        if quantize_output:
            quantize_outputs(output_type, result)

        return result
