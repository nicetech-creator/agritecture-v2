from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math
import timeit

class Gh_Heating:

    @staticmethod
    def info():
        return {
            "name": "gh_heating",
            "description": "Heating system selector in GH models"
        }

    @staticmethod
    def input_type():
        return {
            "heating system size": {
                "description": "Size of the heating system (kW)",
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
                "description": "Total capital expenses cost for heating units ($)",
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
        inputs = parse_inputs(Gh_Heating.input_type(), args)

        output_type = Gh_Heating.output_type()
        result['sql_runtime'] = 0
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        def get_heating_row():
            if inputs['heating system size'] <= 0:
                return {'description': 'None', 'units': 0,
                        'capex_cost': 0, 'depreciation': 0}
            sql = '''
                select
                	id,	description, power, price,
                	ceil(%(required_power)s / power) units,
                	price * ceil(%(required_power)s / power) capex_cost,
                	(maintenance + linear_depreciation) * price * ceil(%(required_power)s / power) depreciation
                from
                	gh_equipment_heating
                order by capex_cost asc
                limit 1
            '''
            query_inputs = {
                'required_power': str(inputs['heating system size'])
            }
            return fetch_one(sql, query_inputs)

        s_start = timeit.default_timer()
        heating_row = get_heating_row()
        result['sql_runtime'] += timeit.default_timer() - s_start
        set_if_unset(result, "description", heating_row["description"])
        set_if_unset(result, "number of units", heating_row["units"])
        set_if_unset(result, "cost", heating_row["capex_cost"])
        set_if_unset(result, "depreciation", heating_row["depreciation"])

        result['runtime'] = timeit.default_timer() - start
        if quantize_output:
            quantize_outputs(output_type, result)

        return result
