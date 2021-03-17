from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math
import timeit

class Gh_Lighting:

    @staticmethod
    def info():
        return {
            "name": "gh_lighting",
            "description": "Lighting system selector for GH"
        }

    @staticmethod
    def input_type():
        return {
            "bedspace": {
                "description": "Bedspace area (sqft)",
                "type": "decimal"
            },
            "ppfd": {
                "description": "Desired photosynthetic photon flux density (umol / m2 s)",
                "default": 200,
                "type": "decimal"
            },
            "electricity cost": {
                "description": "Cost of electricity ($ / kWh)",
                "default": 0.193,
                "type": "decimal"
            },
            "depreciation duration": {
                "description": "Duration of item depreciation",
                "default": 8,
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
                "description": "Number of lights required to cover bedspace",
                "type": "integer"
            },
            "power": {
                "description": "Total power of active lights (kW)",
                "digits": 3,
                "type": "decimal"
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
                "default": 8,
                "type": "decimal"
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def compute(args, quantize_output=False):
        start = timeit.default_timer()
        result = dict()
        inputs = parse_inputs(Gh_Lighting.input_type(), args)

        output_type = Gh_Lighting.output_type()
        result['sql_runtime'] = 0
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        def get_lighting_row():
            if inputs['ppfd'] <= 0:
                return {'description': 'None', 'num_fixtures': 0,
                        'power_overall': 0, 'capex_cost': 0}
            sql = '''
                select
                    id,
                    description,
                    round(power * umol_j * 10.7639 / %(ppfd)s, 2) sqft_per_fixture,
                    round(%(ppfd)s / (umol_j * 10.7639 * 1000), 8) kW_per_sqft,
                    ceil(%(bedspace)s / (power * umol_j * 10.7639 / %(ppfd)s)) num_fixtures,
                    price,
                    price * ceil(%(bedspace)s / (power * umol_j * 10.7639 / %(ppfd)s)) capex_cost,
                    round(%(bedspace)s * (%(ppfd)s / (umol_j * 10.7639 * 1000)), 3) power_overall
                from gh_equipment_lighting
                where id = 4
                limit 1
            '''
            query_inputs = {
                'ppfd': str(inputs['ppfd']),
                'bedspace': str(inputs['bedspace']),
                'kWh_cost': str(inputs['electricity cost'])
            }
            return fetch_one(sql, query_inputs)
        s_start = timeit.default_timer()
        lighting_row = get_lighting_row()
        result['sql_runtime'] += timeit.default_timer() - s_start
        set_if_unset(result, "description", lighting_row["description"])
        set_if_unset(result, "number of units", lighting_row["num_fixtures"])
        set_if_unset(result, "power", lighting_row["power_overall"])
        set_if_unset(result, "cost", lighting_row["capex_cost"])
        set_if_unset(result, "depreciation", result["cost"] / inputs["depreciation duration"])
        result['runtime'] = timeit.default_timer() - start
        if quantize_output:
            quantize_outputs(output_type, result)

        return result