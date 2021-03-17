from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math
import timeit

class Gh_Co2_Injection:

    @staticmethod
    def info():
        return {
            "name": "gh_co2_injection",
            "description": "CO2 injection system selector in GH models"
        }

    @staticmethod
    def input_type():
        return {
            "injection rate": {
                "description": "Selected injection rate (mg / m2 s)",
                "type": "decimal"
            },
            "bedspace": {
                "description": "Bedspace to receive CO2 (sqft)",
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
            "number of units": {
                "description": "Number of units required to provide requested power",
                "type": "integer"
            },
            "cost": {
                "description": "Total capital expenses cost for CO2 enrichment system ($)",
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
        inputs = parse_inputs(Gh_Co2_Injection.input_type(), args)

        output_type = Gh_Co2_Injection.output_type()
        result['sql_runtime'] = 0
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        def get_injection_row():
            if inputs['injection rate'] <= 0:
                return {'description': 'None', 'units': 0,
                        'capex_cost': 0, 'depreciation': 0}
            sql = '''
                select
                    id, description,
                    ceil(%(rate)s / injection_rate) units,
                    ceil(%(rate)s / injection_rate) * price units_cost,
                    distribution_price_per_area * %(bedspace)s distribution_price,
                    ceil(%(rate)s / injection_rate) * price + distribution_price_per_area * %(bedspace)s capex_cost,
                    (maintenance + linear_depreciation) * (ceil(%(rate)s / injection_rate) * price + distribution_price_per_area * %(bedspace)s) depreciation
                from gh_equipment_co2_injection
                order by capex_cost asc
                limit 1
            '''
            query_inputs = {
                'rate': str(inputs['injection rate']),
                'bedspace': str(inputs['bedspace'])
            }
            return fetch_one(sql, query_inputs)

        s_start = timeit.default_timer()
        injection_row = get_injection_row()
        result['sql_runtime'] += timeit.default_timer() - s_start
        set_if_unset(result, "description", injection_row["description"])
        set_if_unset(result, "number of units", injection_row["units"])
        set_if_unset(result, "cost", injection_row["capex_cost"])
        set_if_unset(result, "depreciation", injection_row["depreciation"])

        result['runtime'] = timeit.default_timer() - start
        if quantize_output:
            quantize_outputs(output_type, result)

        return result
