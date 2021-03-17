from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math
import timeit

class Vf_Hvac:

    @staticmethod
    def info():
        return {
            "name": "vf_hvac",
            "description": "HVAC selector for VF"
        }

    @staticmethod
    def input_type():
        return {
            "tons of cooling": {
                "description": "Total cooling (tons of cooling)",
                "type": "decimal"
            },
            "depreciation duration": {
                "description": "Duration of item depreciation",
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
            "description": {
                "description": "Selected equipment description",
                "type": "text"
            },
            "number of units": {
                "description": "Number of HVAC units required for VF volume",
                "type": "integer"
            },
            "seer": {
                "description": "System energy efficiency rating",
                "digits": 4,
                "type": "decimal"
            },
            "power": {
                "description": "Total power of cooling system (kW)",
                "digits": 3,
                "type": "decimal"
            },
            "cost": {
                "description": "Total capital expenses cost for HVAC ($)",
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
                "default": 25,
                "type": "decimal"
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def compute(args, quantize_output=False):
        start = timeit.default_timer()
        result = dict()
        inputs = parse_inputs(Vf_Hvac.input_type(), args)

        output_type = Vf_Hvac.output_type()
        result['sql_runtime'] = 0
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        def get_hvac_row():
            if inputs['tons of cooling'] <= 0:
                return {'description': 'None', 'num_units': 0,
                        'power': 0, 'capex_cost': 0, 'seer': 8}
            sql = '''
                select
                    id,
                    description,
                    SEER seer,
                    12 * %(tons_of_cooling)s / SEER power,
                    price_per_cooling_ton * %(tons_of_cooling)s capex_cost,
                    case when max_cooling_tons_per_unit is null then 1 else %(tons_of_cooling)s / max_cooling_tons_per_unit end num_units
                from vf_equipment_hvac
                order by capex_cost
                limit 1
            '''
            query_inputs = {
                'tons_of_cooling': inputs['tons of cooling']
            }
            return fetch_one(sql, query_inputs)

        s_start = timeit.default_timer()
        hvac_row = get_hvac_row()
        result['sql_runtime'] += timeit.default_timer() - s_start
        set_if_unset(result, "description", hvac_row["description"])
        set_if_unset(result, "number of units", hvac_row["num_units"])
        set_if_unset(result, "seer", hvac_row["seer"])
        set_if_unset(result, "power", hvac_row["power"])
        set_if_unset(result, "cost", hvac_row["capex_cost"])
        set_if_unset(result, "depreciation", result["cost"] / inputs['depreciation duration'])

        result['runtime'] = timeit.default_timer() - start
        if quantize_output:
            quantize_outputs(output_type, result)

        return result