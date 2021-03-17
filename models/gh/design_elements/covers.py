from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math
import timeit

class Gh_Covers_Design_Elements:

    @staticmethod
    def info():
        return {
            "name": "gh_covers_design_elements",
            "description": "Covers / glazing selector design elements in GH models"
        }

    @staticmethod
    def input_type():
        return {
            "element_id": {
                "type": "choice",
                "description": "Selection for covers",
                "default": 1
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def input_values(input_name):
        if input_name == "element_id":
            return fetch_all(
                "SELECT id, description FROM gh_design_elements_covers")
        raise ValueError("Unexpected input: " + str(input_name))

    @staticmethod
    def output_type():
        return {
            "description": {
                "description": "Element description",
                "type": "text"
            },
            "investment per area": {
                "description": "Required investment per area ($ / sqft)",
                "digits": 2,
                "type": "decimal"
            },
            "operating cost": {
                "description": "Estimated operating cost ($ / sqft / year)",
                "digits": 2,
                "type": "decimal"
            },
            "c_leakage": {
                "description": "c_leakage",
                "digits": 5,
                "type": "decimal"
            },
            "transmissivity": {
                "description": "Transmissivity (tau)",
                "digits": 3,
                "type": "decimal"
            },
            "reflectivity": {
                "description": "Reflectivity (p)",
                "digits": 2,
                "type": "decimal"
            },
            "utr": {
                "description": "U_value (W/m2 K)",
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
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def compute(args, quantize_output=False):
        start = timeit.default_timer()
        result = dict()
        inputs = parse_inputs(Gh_Covers_Design_Elements.input_type(), args)

        output_type = Gh_Covers_Design_Elements.output_type()
        result['sql_runtime'] = 0
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        query_inputs = {"element_id": inputs["element_id"]}
        s_start = timeit.default_timer()
        covers_row = fetch_one(
            "SELECT description, investment_per_area, (maintenance + linear_depreciation) * investment_per_area operating_cost, c_leakage, transmissivity, reflectivity, u_value FROM gh_design_elements_covers WHERE id=%(element_id)s",
            query_inputs)
        result['sql_runtime'] += timeit.default_timer() - s_start
        if covers_row is None:
            raise ValueError(
                "No elements found for inputs: " +
                str(query_inputs))

        set_if_unset(result, "description", covers_row["description"])
        set_if_unset(
            result,
            "investment per area",
            covers_row["investment_per_area"])
        set_if_unset(result, "operating cost", covers_row["operating_cost"])
        set_if_unset(result, "c_leakage", covers_row["c_leakage"])
        set_if_unset(result, "transmissivity", covers_row["transmissivity"])
        set_if_unset(result, "reflectivity", covers_row["reflectivity"])
        set_if_unset(result, "utr", covers_row["u_value"])
        result['runtime'] = timeit.default_timer() - start
        if quantize_output:
            quantize_outputs(output_type, result)

        return result
