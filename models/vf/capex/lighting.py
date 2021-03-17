from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math
import timeit

class Vf_Lighting:

    @staticmethod
    def info():
        return {
            "name": "vf_lighting",
            "description": "Lighting system selector for VF"
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
                "default": 300,
                "type": "decimal"
            },
            "electricity cost": {
                "description": "Cost of electricity ($ / kWh)",
                "default": 0.193,
                "type": "decimal"
            },
            "lighting hours": {
                "description": "Projected use of lighting per year (h / y)",
                "default": 6000,
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
            "photosynthetic efficacy": {
                "description": "Light system photosynthetic efficacy (umol / J)",
                "digits": 2,
                "type": "decimal"
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
        inputs = parse_inputs(Vf_Lighting.input_type(), args)

        output_type = Vf_Lighting.output_type()
        result['sql_runtime'] = 0
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        def get_lighting_row():
            if inputs['ppfd'] <= 0:
                return {'description': 'None', 'num_fixtures': 0,
                        'power_overall': 0, 'capex_cost': 0, 'umol_j': 2.17}
            sql = '''
                select
                    id,
                    description,
                    umol_j,
                    round(power * umol_j * 10.7639 / %(ppfd)s, 2) sqft_per_fixture,
                    round(%(ppfd)s / (umol_j * 10.7639 * 1000), 8) kW_per_sqft,
                    ceil(%(bedspace)s / (power * umol_j * 10.7639 / %(ppfd)s)) num_fixtures,
                    price,
                    price * ceil(%(bedspace)s / (power * umol_j * 10.7639 / %(ppfd)s)) capex_cost,
                    round(%(bedspace)s * (%(ppfd)s / (umol_j * 10.7639 * 1000)), 3) power_overall,
                    round(%(bedspace)s * (%(ppfd)s / (umol_j * 10.7639 * 1000)) * %(kWh_cost)s, 2) opex_cost_per_lighting_hour,
                    round(%(bedspace)s * (%(ppfd)s / (umol_j * 10.7639 * 1000)) * %(kWh_cost)s * %(lighting_hours)s, 2) opex_cost_per_year
                from vf_equipment_lighting
                order by kW_per_sqft asc
                limit 1
            '''
            query_inputs = {
                'ppfd': str(inputs['ppfd']),
                'bedspace': str(inputs['bedspace']),
                'kWh_cost': str(inputs['electricity cost']),
                'lighting_hours': str(inputs['lighting hours'])
            }
            return fetch_one(sql, query_inputs)

        s_start = timeit.default_timer()
        lighting_row = get_lighting_row()
        result['sql_runtime'] += timeit.default_timer() - s_start
        set_if_unset(result, "description", lighting_row["description"])
        set_if_unset(result, "number of units", lighting_row["num_fixtures"])
        set_if_unset(result, "photosynthetic efficacy", lighting_row["umol_j"])
        set_if_unset(result, "power", lighting_row["power_overall"])
        set_if_unset(result, "cost", lighting_row["capex_cost"])
        set_if_unset(result, "depreciation", result["cost"] / inputs['depreciation duration'])
        result['runtime'] = timeit.default_timer() - start
        if quantize_output:
            quantize_outputs(output_type, result)

        return result