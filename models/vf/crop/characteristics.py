from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math
import timeit

class Vf_Crop_Characteristics:

    @staticmethod
    def info():
        return {
            "name": "vf_crop_characteristics",
            "description": "Crop characteristics matrix selector for VF"
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
                    "optimal avg ppfd": {
                        "digits": 0,
                        "description": "Optimal average PPFD (photosynthetic photon flux density) (umol/m2 s)",
                        "type": "decimal"
                    },
                    "optimal dli": {
                        "digits": 0,
                        "description": "Optimal DLI (day light integral)",
                        "type": "decimal"
                    },
                    "light sum": {
                        "digits": 0,
                        "description": "Light sum (mol / m2)",
                        "type": "decimal"
                    },
                    "crop difficulty": {
                        "digits": 0,
                        "description": "Projected crop difficulty",
                        "type": "decimal"
                    },
                    "plant density": {
                        "digits": 4,
                        "description": "Plant density (plants / sqft)",
                        "type": "decimal"
                    },
                    "seed price": {
                        "digits": 2,
                        "type": "decimal"
                    }
                }
            },
            "max optimal avg ppfd": {
                "digits": 0,
                "description": "Max optimal average PPFD (photosynthetic photon flux density) (umol/m2 s)",
                "type": "decimal"
            },
            "avg optimal dli": {
                "digits": 0,
                "description": "Average optimal DLI (day light integral)",
                "type": "decimal"
            },
            "combined crop cycle": {
                "digits": 0,
                "description": "Combined crop cycle duration (d)",
                "type": "decimal"
            },
            "combined difficulty factor": {
                "digits": 0,
                "description": "Combined crop difficulty factor",
                "type": "decimal"
            },
            "combined seed price": {
                "digits": 2,
                "type": "decimal"
            },
            "combined substrate cost": {
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
        inputs = parse_inputs(Vf_Crop_Characteristics.input_type(), args)

        output_type = Vf_Crop_Characteristics.output_type()
        result['sql_runtime'] = 0
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        sql = "SELECT * FROM crops WHERE is_vf_crop = 1 and id=%(id)s"
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
            line['optimal crop cycle'] = row['optimal_crop_cycle']
            line['cycles per year'] = 365 / row['optimal_crop_cycle']
            line['optimal avg ppfd'] = row['optimal_avg_ppfd__umol_m2_s']
            line['optimal dli'] = row['optimal_dli__mol_m2_d']
            line['light sum'] = row['light_sum__mol_m2']
            line['crop difficulty'] = row['difficulty_soilless']
            line['plant density'] = row['plants_per_sqft']
            line['seed price'] = 0.01 if row['plants_per_sqft'] > 0 else 0.19
            result_crops.append(line)
        result['crops'] = result_crops

        sum_system_fraction = sum(line["system fraction"]
                                  for line in result["crops"])
        set_if_unset(result, "max optimal avg ppfd", max(
            line["optimal avg ppfd"] for line in result["crops"]))
        set_if_unset(
            result,
            "avg optimal dli",
            sum(
                line["optimal dli"] *
                line["system fraction"] for line in result["crops"]) /
            sum_system_fraction)
        set_if_unset(result, "combined difficulty factor", 2 *
                     sum(line["crop difficulty"] *
                         line["system fraction"] for line in result["crops"]) /
                     sum_system_fraction)

        def nvl(x, default_value):
            return x if x is not None else default_value
        # Approximate combined crop cycle as average
        sum_system_fractions = sum([line['system fraction']
                                    for line in result['crops']])
        approx_cycles_per_year = sum([line['system fraction'] * 365 / nvl(
            line['optimal crop cycle'], 45.5) for line in result['crops']]) / sum_system_fractions
        crop_cycle = 365 / approx_cycles_per_year
        total_seed_cost = sum(
            [
                line['system fraction'] *
                line['seed price'] *
                line['cycles per year'] *
                line['plant density'] for line in result['crops'] if line['plant density'] != 0])
        total_seeds = sum([line['system fraction'] * line['cycles per year'] * line['plant density']
                           for line in result['crops'] if line['plant density'] != 0])
        combined_seed_price = 0 if total_seed_cost == 0 else total_seed_cost / total_seeds
        total_substrate_cost = sum(
            [
                line['system fraction'] *
                line['seed price'] *
                line['cycles per year'] for line in result['crops'] if line['plant density'] == 0])
        total_substrate = sum([line['system fraction'] * line['cycles per year']
                               for line in result['crops'] if line['plant density'] == 0])
        combined_substrate_cost = 0 if total_substrate == 0 else total_substrate_cost / total_substrate

        set_if_unset(result, "combined crop cycle", crop_cycle)
        set_if_unset(result, "combined seed price", combined_seed_price)
        set_if_unset(
            result,
            "combined substrate cost",
            combined_substrate_cost)
        result['runtime'] = timeit.default_timer() - start
        if quantize_output:
            quantize_outputs(output_type, result)

        return result
