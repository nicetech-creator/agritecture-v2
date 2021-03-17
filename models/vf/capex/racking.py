from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math
import timeit

class Vf_Racking:

    @staticmethod
    def info():
        return {
            "name": "vf_racking",
            "description": "Racking selector for VF"
        }

    @staticmethod
    def input_type():
        return {
            "number of levels": {
                "description": "Actual number of levels in system",
                "type": "decimal"
            },
            "bedspace": {
                "description": "Bedspace area (sqft)",
                "type": "decimal"
            },
            "price_per_sft": {
                "description": "Price per square ft",
                "default": -1,
                "digits": 2,
                "type": "decimal"
            },
            "depreciation duration": {
                "description": "Duration of item depreciation",
                "default": 50,
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
            "description": {
                "description": "Selected equipment description",
                "type": "text"
            },
            "cost": {
                "description": "Total capital expenses cost for lights ($)",
                "digits": 2,
                "type": "decimal"
            },
            "depreciation": {
                "description": "Depreciation cost per year ($ / y)",
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
            },
            "depreciation duration": {
                "description": "Duration of item depreciation",
                "default": 50,
                "type": "decimal"
            },
            "price_per_sft": {
                "description": "Price per square ft",
                "default": 0,
                "digits": 2,
                "type": "decimal"
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def compute(args, quantize_output=False):
        start = timeit.default_timer()
        result = dict()
        inputs = parse_inputs(Vf_Racking.input_type(), args)

        output_type = Vf_Racking.output_type()
        result['sql_runtime'] = 0
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        def get_racking_row():
            if inputs['number of levels'] <= 0 or inputs['bedspace'] <= 0:
                return {'description': 'None', 'total_price': 0}
            sql = '''
                select
                    id,
                    description,
                    shelf_width_ext * shelf_length floor_area_per_rack,
                    shelf_length * shelf_width_int * %(number_of_levels)s bedspace_per_rack,
                    price_per_area * shelf_length * shelf_width_int * %(number_of_levels)s price_per_rack,
                    price_per_area * shelf_width_int * %(number_of_levels)s / shelf_width_ext price_per_floor_area,
                    price_per_area * %(bedspace)s total_price
                from vf_equipment_racking
                order by total_price
                limit 1
            '''
            query_inputs = {
                'number_of_levels': inputs['number of levels'],
                'bedspace': inputs['bedspace']
            }
            return fetch_one(sql, query_inputs)

        s_start = timeit.default_timer()
        racking_row = get_racking_row()
        result['sql_runtime'] += timeit.default_timer() - s_start
        set_if_unset(result, "description", racking_row["description"])
        set_if_unset(result, "cost", racking_row["total_price"] if inputs["price_per_sft"] < 0 else inputs["bedspace"] * inputs["price_per_sft"])
        result["price_per_sft"] = racking_row["total_price"] / inputs['bedspace']  if inputs["price_per_sft"] < 0 else inputs["price_per_sft"]
        set_if_unset(result, "depreciation", result["cost"] / inputs['depreciation duration'])

        result['runtime'] = timeit.default_timer() - start
        if quantize_output:
            quantize_outputs(output_type, result)

        return result