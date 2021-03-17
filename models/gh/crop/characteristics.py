from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math
import timeit

class Gh_Crop_Characteristics:

    @staticmethod
    def info():
        return {
            "name": "gh_crop_characteristics",
            "description": "Crop characteristics matrix selector for GH"
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
                    },
                    "grow system description": {
                        "description": "Name of selected grow system",
                        "type": "text"
                    }
                }
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def input_values(input_name):
        if input_name == "crops.id":
            return fetch_all(
                "SELECT id, crop_type FROM crops WHERE is_gh_crop = 1 ORDER BY id")
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
                    "max unlimited assimilation": {
                        "digits": 1,
                        "description": "Max unlimited assimilation (mg/m2 s)",
                        "type": "decimal"
                    },
                    "degree days per cycle": {
                        "digits": 1,
                        "description": "Degree days (C d) per cycle",
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
                    "lai": {
                        "digits": 0,
                        "type": "decimal"
                    },
                    "r_b": {
                        "digits": 0,
                        "type": "decimal"
                    },
                    "r_s_min": {
                        "digits": 0,
                        "type": "decimal"
                    },
                    "r_s_min_night": {
                        "digits": 0,
                        "type": "decimal"
                    },
                    "alpha_crop": {
                        "digits": 1,
                        "type": "decimal"
                    },
                    "l_s": {
                        "digits": 2,
                        "type": "decimal"
                    },
                    "l_i": {
                        "digits": 2,
                        "type": "decimal"
                    },
                    "min crop temperature": {
                        "digits": 0,
                        "description": "Basal daily development temperature (C)",
                        "type": "decimal"
                    },
                    "max crop temperature": {
                        "digits": 0,
                        "description": "Max daily development temperature (C)",
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
            "combined min crop temperature": {
                "digits": 1,
                "description": "Combined min crop temperature (C)",
                "type": "decimal"
            },
            "combined max crop temperature": {
                "digits": 1,
                "description": "Combined max crop temperature (C)",
                "type": "decimal"
            },
            "combined degree days per cycle": {
                "digits": 1,
                "description": "Combined degree days (C d) per cycle",
                "type": "decimal"
            },
            "combined max unlimited assimilation": {
                "digits": 1,
                "description": "Combined max unlimited assimilation (mg/m2 s)",
                "type": "decimal"
            },
            "combined difficulty factor": {
                "digits": 0,
                "description": "Combined crop difficulty factor",
                "type": "decimal"
            },
            "combined lai": {
                "digits": 0,
                "type": "decimal"
            },
            "combined r_b": {
                "digits": 0,
                "type": "decimal"
            },
            "combined r_s_min": {
                "digits": 0,
                "type": "decimal"
            },
            "combined r_s_min_night": {
                "digits": 0,
                "type": "decimal"
            },
            "combined alpha_crop": {
                "digits": 1,
                "type": "decimal"
            },
            "combined l_s": {
                "digits": 2,
                "type": "decimal"
            },
            "combined l_i": {
                "digits": 2,
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
        inputs = parse_inputs(Gh_Crop_Characteristics.input_type(), args)

        output_type = Gh_Crop_Characteristics.output_type()
        result['sql_runtime'] = 0
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        result_crops = []
        sql = "SELECT * FROM crops WHERE is_gh_crop = 1 and id=%(id)s"
        for input_row in inputs['crops']:
            is_soil = input_row['grow system description'].lower() == "soil"
            s_start = timeit.default_timer()
            row = fetch_one(sql, {'id': input_row['id']})
            result['sql_runtime'] += timeit.default_timer() - s_start
            assert row is not None, 'Could not find crop with ID: ' + \
                str(input_row['id'])
            line = dict()
            line['id'] = input_row['id']
            line['system fraction'] = input_row['system fraction']
            line['optimal crop cycle'] = row['optimal_crop_cycle']
            line['optimal avg ppfd'] = row['optimal_avg_ppfd__umol_m2_s']
            line['optimal dli'] = row['optimal_dli__mol_m2_d']
            line['light sum'] = row['light_sum__mol_m2']
            line['max unlimited assimilation'] = row['max_unlimited_assimilation__mg_m2_s']
            line['degree days per cycle'] = row['degree_days__c_d__per_dev_cycle']
            line['crop difficulty'] = row['difficulty_soil'] if is_soil else row['difficulty_soilless']
            line['plant density'] = row['plants_per_sqft']
            line['lai'] = row['lai']
            line['r_b'] = row['r_b']
            line['r_s_min'] = row['r_s_min']
            line['r_s_min_night'] = None if row['r_s_min'] is None else row['r_s_min'] + 3000
            line['alpha_crop'] = row['alpha_crop']
            line['l_s'] = row['l_s']
            line['l_i'] = row['l_i']
            line['min crop temperature'] = row['basal_daily_development_temperature__c']
            line['max crop temperature'] = row['max_daily_development_temperature__c']
            line['seed price'] = 0.01 if row['seed_price_per_unit'] is None else row['seed_price_per_unit']
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

        def argmax(items, compare_field, select_field, default_value):
            filtered_items = list(
                filter(
                    lambda x: x[select_field] is not None and x[compare_field] is not None,
                    items))
            if len(filtered_items) == 0:
                return default_value
            return max(filtered_items, key=lambda x: x[compare_field])[
                select_field]

        def nvl(x, default_value):
            return x if x is not None else default_value
        # Select the non-null value from the element with largest system fraction for combined crop characteristics
        # Default values hardcoded below are only used if no values can be
        # found for any of the selected crops
        lai = argmax(result['crops'], 'system fraction', 'lai', 3)
        r_b = argmax(result['crops'], 'system fraction', 'r_b', 200)
        r_s_min = argmax(result['crops'], 'system fraction', 'r_s_min', 150)
        r_s_min_night = argmax(
            result['crops'],
            'system fraction',
            'r_s_min_night',
            3150)
        alpha_crop = argmax(
            result['crops'],
            'system fraction',
            'alpha_crop',
            0.4)
        l_s = argmax(result['crops'], 'system fraction', 'l_s', 0.85)
        l_i = argmax(result['crops'], 'system fraction', 'l_i', -0.37)
        # Approximate characteristics based on crop with largest system
        # fraction
        crop_min_temperature = argmax(
            result['crops'],
            'system fraction',
            'min crop temperature',
            5)
        crop_max_temperature = argmax(
            result['crops'],
            'system fraction',
            'max crop temperature',
            25)
        max_unlimited_assimilation = argmax(
            result['crops'], 'system fraction', 'max unlimited assimilation', 2)
        # Approximate combined crop cycle as average
        sum_system_fractions = sum([line['system fraction']
                                    for line in result['crops']])
        approx_cycles_per_year = sum([line['system fraction'] * 365 / nvl(
            line['optimal crop cycle'], 45.5) for line in result['crops']]) / sum_system_fractions
        crop_cycle = 365 / approx_cycles_per_year
        # Approximate degree days per cycle based on combined cycles
        approx_degree_days_per_year = sum([line['system fraction'] * nvl(line['degree days per cycle'], 320) * 365 / nvl(
            line['optimal crop cycle'], 45.5) for line in result['crops']]) / sum_system_fractions
        degree_days_per_cycle = approx_degree_days_per_year / approx_cycles_per_year
        total_seed_cost = sum([line['system fraction'] *
                               line['seed price'] *
                               (365 /
                                line['optimal crop cycle']) *
                               line['plant density'] for line in result['crops'] if line['plant density'] != 0])
        total_seeds = sum([line['system fraction'] * (365 / line['optimal crop cycle']) *
                           line['plant density'] for line in result['crops'] if line['plant density'] != 0])
        combined_seed_price = 0 if total_seed_cost == 0 else total_seed_cost / total_seeds
        total_substrate_cost = sum([line['system fraction'] * line['seed price'] * (
            365 / line['optimal crop cycle']) for line in result['crops'] if line['plant density'] == 0])
        total_substrate = sum([line['system fraction'] * (365 / line['optimal crop cycle'])
                               for line in result['crops'] if line['plant density'] == 0])
        combined_substrate_cost = 0 if total_substrate == 0 else total_substrate_cost / total_substrate

        set_if_unset(result, "combined lai", lai)
        set_if_unset(result, "combined r_b", r_b)
        set_if_unset(result, "combined r_s_min", r_s_min)
        set_if_unset(result, "combined r_s_min_night", r_s_min_night)
        set_if_unset(result, "combined alpha_crop", alpha_crop)
        set_if_unset(result, "combined l_s", l_s)
        set_if_unset(result, "combined l_i", l_i)
        set_if_unset(result, "combined crop cycle", crop_cycle)
        set_if_unset(
            result,
            "combined min crop temperature",
            crop_min_temperature)
        set_if_unset(
            result,
            "combined max crop temperature",
            crop_max_temperature)
        set_if_unset(
            result,
            "combined degree days per cycle",
            degree_days_per_cycle)
        set_if_unset(
            result,
            "combined max unlimited assimilation",
            max_unlimited_assimilation)
        set_if_unset(result, "combined seed price", combined_seed_price)
        set_if_unset(
            result,
            "combined substrate cost",
            combined_substrate_cost)
        result['runtime'] = timeit.default_timer() - start
        if quantize_output:
            quantize_outputs(output_type, result)

        return result
