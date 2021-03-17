from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math
import timeit

class Vf_Input_Land_Status:

    @staticmethod
    def info():
        return {
            "name": "vf_input_land_status",
            "description": "Input translator for VF land status"
        }

    @staticmethod
    def input_type():
        return {
            "land status": {
                "type": "choice",
                "description": "Selection land status",
                "default": 1
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def input_values(input_name):
        if input_name == "land status":
            return fetch_all("SELECT id, description FROM land_status")
        raise ValueError("Unexpected input: " + str(input_name))

    @staticmethod
    def output_type():
        return {
            "is rent": {
                "description": "Whether the land is rented",
                "type": "bool"
            },
            "is buy": {
                "description": "Whether the land is being bought",
                "type": "bool"
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
        inputs = parse_inputs(Vf_Input_Land_Status.input_type(), args)

        output_type = Vf_Input_Land_Status.output_type()
        result['sql_runtime'] = 0
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        query_inputs = {"element_id": inputs["land status"]}
        s_start = timeit.default_timer()
        land_status_row = fetch_one(
            "SELECT is_rent, is_buy FROM land_status WHERE id=%(element_id)s",
            query_inputs)
        result['sql_runtime'] += timeit.default_timer() - s_start
        if land_status_row is None:
            raise ValueError(
                "No elements found for inputs: " +
                str(query_inputs))

        set_if_unset(result, "is rent", land_status_row["is_rent"] != 0)
        set_if_unset(result, "is buy", land_status_row["is_buy"] != 0)
        result['runtime'] = timeit.default_timer() - start
        if quantize_output:
            quantize_outputs(output_type, result)

        return result
