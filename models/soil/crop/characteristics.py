from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math


class Soil_Crop_Characteristics:

    @staticmethod
    def info():
        return {
            "name": "soil_crop_characteristics",
            "description": "Crop characteristics matrix selector for Soil"
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
            return fetch_all("SELECT id, crop_type FROM crops ORDER BY id")
        raise ValueError("Unexpected input: " + str(input_name))

    @staticmethod
    def output_type():
        return {
            "crops": {
                "type": "list",
                "description": "Crop matrix results",
                "element type": {
                    "id": {
                        "type": "integer",
                        "description": "Crop index",
                        "primary key": True
                    },
                    "optimal crop cycle": {
                        "digits": 1,
                        "description": "Optimal crop cycle duration (d)",
                        "type": "decimal"
                    },
                    "cycles per year": {
                        "digits": 1,
                        "description": "Number of cycles per year",
                        "type": "decimal"
                    },
                    "crop difficulty": {
                        "digits": 0,
                        "description": "Projected crop difficulty",
                        "type": "decimal"
                    },
                    "absolute min temp": {
                        "digits": 1,
                        "description": "Absolute min temperature for crop (C)",
                        "type": "decimal"
                    },
                    "optimal min temp": {
                        "digits": 1,
                        "description": "Optimal min temperature for crop (C)",
                        "type": "decimal"
                    },
                    "optimal max temp": {
                        "digits": 1,
                        "description": "Optimal max temperature for crop (C)",
                        "type": "decimal"
                    },
                    "absolute max temp": {
                        "digits": 1,
                        "description": "Absolute max temperature for crop (C)",
                        "type": "decimal"
                    }
                }
            },
            "combined difficulty factor": {
                "digits": 0,
                "description": "Combined crop difficulty factor",
                "type": "decimal"
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def compute(args, quantize_output=False):
        result = dict()
        inputs = parse_inputs(Soil_Crop_Characteristics.input_type(), args)

        output_type = Soil_Crop_Characteristics.output_type()
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        sql = "SELECT * FROM crops WHERE id=%(id)s"
        result_crops = []
        for input_row in inputs['crops']:
            row = fetch_one(sql, {'id': input_row['id']})
            assert row is not None, 'Could not find crop with ID: ' + \
                str(input_row['id'])
            line = dict()
            line['id'] = input_row['id']
            line['system fraction'] = input_row['system fraction']
            line['optimal crop cycle'] = row['optimal_crop_cycle']
            line['cycles per year'] = 365 / row['optimal_crop_cycle']
            line['crop difficulty'] = row['difficulty_soil']
            line['absolute min temp'] = row['temp_absolute_min']
            line['optimal min temp'] = row['temp_optimal_min']
            line['optimal max temp'] = row['temp_optimal_max']
            line['absolute max temp'] = row['temp_absolute_max']
            result_crops.append(line)
        result['crops'] = result_crops

        set_if_unset(result, "combined difficulty factor", 2 *
                     sum(line["crop difficulty"] *
                         line["system fraction"] for line in result["crops"]) /
                     sum(line["system fraction"] for line in result["crops"]))

        if quantize_output:
            quantize_outputs(output_type, result)

        return result
