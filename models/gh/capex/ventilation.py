from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math
import timeit

class Gh_Ventilation:

    @staticmethod
    def info():
        return {
            "name": "gh_ventilation",
            "description": "Mechanical ventilation selector in GH models"
        }

    @staticmethod
    def input_type():
        return {
            "ventilation total cfm": {
                "description": "Total ventilation rate in cubic feet per minute (CFM)",
                "type": "decimal"
            },
            "ventilation time": {
                "description": "Time ventilation is active in hours per year (h / y)",
                "type": "decimal"
            },
            "electricity cost": {
                "description": "Cost of electricity ($ / kWh)",
                "default": 0.193,
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
            "power": {
                "description": "Total power of active ventilation system (kWh / y)",
                "digits": 3,
                "type": "decimal"
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
        inputs = parse_inputs(Gh_Ventilation.input_type(), args)

        output_type = Gh_Ventilation.output_type()
        result['sql_runtime'] = 0
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        def get_ventilation_row():
            if inputs['ventilation time'] <= 0 or inputs['ventilation total cfm'] <= 0:
                return {'description': 'None', 'units': 0,
                        'power_overall': 0, 'capex_cost': 0, 'depreciation': 0}
            sql = '''
                select
                    id, description,
                    ceil(%(total_cfm)s / cfm) units,
                    ceil(%(total_cfm)s / cfm) * power * %(ventilation_hours)s power_overall,
                    ceil(%(total_cfm)s / cfm) * price capex_cost,
                    (maintenance + linear_depreciation) * ceil(%(total_cfm)s / cfm) * price depreciation,
                    round(ceil(%(total_cfm)s / cfm) * price * (maintenance + linear_depreciation), 2) capex_decay_cost_per_year,
                    round(ceil(%(total_cfm)s / cfm) * power * %(ventilation_hours)s * %(kWh_cost)s, 2) electricity_cost_per_year,
                    round(ceil(%(total_cfm)s / cfm) * (price * (maintenance + linear_depreciation) + power * %(ventilation_hours)s * %(kWh_cost)s), 2) opex_cost_per_year
                from gh_equipment_ventilation
                order by opex_cost_per_year asc
                limit 1
            '''
            query_inputs = {
                'total_cfm': str(inputs['ventilation total cfm']),
                'ventilation_hours': str(inputs['ventilation time']),
                'kWh_cost': str(inputs['electricity cost'])
            }
            return fetch_one(sql, query_inputs)
        s_start = timeit.default_timer()
        ventilation_row = get_ventilation_row()
        result['sql_runtime'] += timeit.default_timer() - s_start
        set_if_unset(result, "description", ventilation_row["description"])
        set_if_unset(result, "number of units", ventilation_row["units"])
        set_if_unset(result, "power", ventilation_row["power_overall"])
        set_if_unset(result, "cost", ventilation_row["capex_cost"])
        set_if_unset(result, "depreciation", ventilation_row["depreciation"])
        result['runtime'] = timeit.default_timer() - start
        if quantize_output:
            quantize_outputs(output_type, result)

        return result
