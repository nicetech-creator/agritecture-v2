from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math
import timeit

class Vf_Energy_Setpoint_Selection:

    @staticmethod
    def info():
        return {
            "name": "vf_energy_setpoint_selection",
            "description": "Temperature and humidity setpoint selection for VF"
        }

    @staticmethod
    def input_type():
        return {
            "crops": {
                "type": "list",
                "description": "Selection of crops to compute",
                "element type": {
                    "id": {
                        "type": "choice",
                        "description": "Selected crop item",
                        "primary key": True
                    },
                    "system fraction": {
                        "description": "Fraction of bedspace allocated to crop",
                        "type": "decimal"
                    }
                }
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def input_values(input_name):
        if input_name == "crops.id":
            return fetch_all(
                "SELECT id, crop_type FROM crops WHERE is_vf_crop = 1 ORDER BY id")
        raise ValueError("Unexpected input: " + str(input_name))

    @staticmethod
    def output_type():
        return {
            "inside day temperature": {
                "description": "Setpoint for inside day temperature (C)",
                "digits": 0,
                "type": "decimal"
            },
            "inside night temperature": {
                "description": "Setpoint for inside night temperature (C)",
                "digits": 0,
                "type": "decimal"
            },
            "inside day relative humidity": {
                "description": "Setpoint for inside day relative humidity (%)",
                "digits": 2,
                "type": "decimal"
            },
            "inside night relative humidity": {
                "description": "Setpoint for inside night relative humidity (%)",
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
        inputs = parse_inputs(Vf_Energy_Setpoint_Selection.input_type(), args)

        output_type = Vf_Energy_Setpoint_Selection.output_type()
        result['sql_runtime'] = 0
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        sql = "SELECT id, coalesce(warm_environment_crop, 0) warm_environment_crop FROM crops WHERE is_vf_crop = 1 and id=%(id)s ORDER BY id"
        result_crops = []
        for input_row in inputs['crops']:
            s_start = timeit.default_timer()
            row = fetch_one(sql, {'id': input_row['id']})
            result['sql_runtime'] += timeit.default_timer() - s_start
            assert row is not None, 'Could not find crop with ID: ' + \
                str(input_row['id'])
            line = dict()
            line['id'] = input_row['id']
            line['system fraction'] = input_row['system fraction']
            line['warm environment crop'] = row['warm_environment_crop']
            result_crops.append(line)
        result['crops'] = result_crops

        denominator = sum(line['system fraction'] for line in result['crops'])
        if denominator == 0:
            denominator = 1
        warm_fraction = sum(line['system fraction'] * line['warm environment crop']
                            for line in result['crops']) / denominator

        set_if_unset(result, "inside day temperature", ((
            70 * (1 - warm_fraction) + 81 * warm_fraction) - 32) * 5 / 9)
        set_if_unset(result, "inside night temperature", ((
            62 * (1 - warm_fraction) + 70 * warm_fraction) - 32) * 5 / 9)
        set_if_unset(result, "inside day relative humidity",
                     0.6 * (1 - warm_fraction) + 0.65 * warm_fraction)
        set_if_unset(result, "inside night relative humidity",
                     0.6 * (1 - warm_fraction) + 0.65 * warm_fraction)

        result['runtime'] = timeit.default_timer() - start
        if quantize_output:
            quantize_outputs(output_type, result)

        return result
