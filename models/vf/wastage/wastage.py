from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math
import timeit

class Vf_Wastage:

    @staticmethod
    def info():
        return {
            "name": "vf_wastage",
            "description": "Wastage model for VF"
        }

    @staticmethod
    def input_type():
        return {
            "grower experience": {
                "type": "choice",
                "description": "Selection for grower experience",
                "default": 1
            },
            "base wastage": {
                "description": "Base wastage constant for calculation",
                "default": 0.1,
                "type": "decimal"
            },
            "organic production": {
                "description": "Whether the system will use organic production",
                "type": "bool",
                "default": False
            },
            "crop base difficulty": {
                "description": "Base difficulty for crop combination",
                "default": 2,
                "type": "decimal"
            },
            "number of crops": {
                "description": "Number of crops being grown",
                "default": 3,
                "type": "decimal"
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def input_values(input_name):
        if input_name == "grower experience":
            return fetch_all(
                "SELECT id, description FROM vf_grower_experience")
        raise ValueError("Unexpected input: " + str(input_name))

    @staticmethod
    def output_type():
        return {
            "wastage schedule": {
                "type": "list",
                "description": "Projected wastage for each year",
                "element type": {
                    "year": {
                        "type": "integer",
                        "description": "Year for projection"
                    },
                    "wastage": {
                        "digits": 4,
                        "description": "Projected wastage for this year",
                        "type": "decimal"
                    }
                }
            },
            "initial wastage": {
                "description": "Projected value for wastage on first year",
                "digits": 4,
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
        inputs = parse_inputs(Vf_Wastage.input_type(), args)

        output_type = Vf_Wastage.output_type()
        result['sql_runtime'] = 0
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        query_inputs = {"element_id": inputs["grower experience"]}
        s_start = timeit.default_timer()
        grower_experience_row = fetch_one(
            "SELECT base_wastage w FROM vf_grower_experience WHERE id=%(element_id)s",
            query_inputs)
        result['sql_runtime'] += timeit.default_timer() - s_start
        if grower_experience_row is None:
            raise ValueError(
                "No elements found for inputs: " +
                str(query_inputs))

        def difficulty_from_crop_number(num_crops):
            if num_crops < 2:
                return num_crops
            return 2 + (num_crops - 2) / 4

        def wastage_reduction_by_year(year):
            if year <= 3:
                return 0.1
            if year <= 7:
                return 0.08
            return 0.06

        crop_base_difficulty = inputs["crop base difficulty"] / 100
        organic_factor_difficulty = 1.5 if inputs["organic production"] else 1
        number_of_crops_difficulty = difficulty_from_crop_number(
            inputs["number of crops"])
        crop_difficulty_w_factors = crop_base_difficulty * \
            organic_factor_difficulty * number_of_crops_difficulty
        initial_wastage = (
            inputs["base wastage"] + grower_experience_row["w"]) * (1 + crop_difficulty_w_factors)
        set_if_unset(result, "initial wastage", initial_wastage)
        wastage_schedule = []
        current_wastage = initial_wastage
        for i in range(1, 16):
            wastage_schedule.append({
                'year': i,
                'wastage': current_wastage
            })
            current_wastage *= (1 - wastage_reduction_by_year(i + 1))
        result['wastage schedule'] = wastage_schedule

        result['runtime'] = timeit.default_timer() - start
        if quantize_output:
            quantize_outputs(output_type, result)

        return result
