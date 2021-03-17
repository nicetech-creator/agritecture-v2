from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math
import timeit

class Gh_Cooling:

    @staticmethod
    def info():
        return {
            "name": "gh_cooling",
            "description": "Evaporative cooling system selector for GH"
        }

    @staticmethod
    def input_type():
        return {
            "cooling rate": {
                "description": "Selected cooling rate (exchanges / min)",
                "type": "decimal"
            },
            "area": {
                "description": "Area to be cooled (sqft)",
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
                "description": "Total capital expenses cost for cooling unit ($)",
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
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def compute(args, quantize_output=False):
        start = timeit.default_timer()
        result = dict()
        inputs = parse_inputs(Gh_Cooling.input_type(), args)

        output_type = Gh_Cooling.output_type()
        result['sql_runtime'] = 0
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        def get_cooling_row():
            if inputs['cooling rate'] <= 0:
                return {'description': 'None',
                        'combined_price_per_area': 0, 'depreciation_per_area': 0}
            sql = '''
                select
                    id, description, power,
                    ceil(%(rate)s / power) required_parallel_units,
                    price_per_area * ceil(%(rate)s / power) combined_price_per_area,
                    (maintenance + linear_depreciation) * price_per_area * ceil(%(rate)s / power) depreciation_per_area
                from
                    gh_equipment_cooling
                where
                    price_per_area is not null and power is not null
                order by combined_price_per_area asc
                limit 1
            '''
            query_inputs = {
                'rate': str(inputs['cooling rate'])
            }
            return fetch_one(sql, query_inputs)

        s_start = timeit.default_timer()
        cooling_row = get_cooling_row()
        result['sql_runtime'] += timeit.default_timer() - s_start
        set_if_unset(result, "description", cooling_row["description"])
        set_if_unset(
            result,
            "cost",
            cooling_row["combined_price_per_area"] *
            inputs["area"])
        set_if_unset(
            result,
            "depreciation",
            cooling_row["depreciation_per_area"] *
            inputs["area"])

        result['runtime'] = timeit.default_timer() - start
        if quantize_output:
            quantize_outputs(output_type, result)

        return result
