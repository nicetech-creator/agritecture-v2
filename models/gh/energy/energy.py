from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math
import timeit

class Gh_Energy:

    @staticmethod
    def info():
        return {
            "name": "gh_energy",
            "description": "Energy calculation for GH"
        }

    @staticmethod
    def input_type():
        return {
            "latitude": {
                "description": "geographical latitude",
                "type": "decimal"
            },
            "longitude": {
                "description": "geographical longitude",
                "type": "decimal"
            },
            "area": {
                "description": "Total greenhouse area (sqft)",
                "type": "decimal"
            },
            "volume": {
                "description": "Total greenhouse volume (cuft)",
                "type": "decimal"
            },
            "cover to footprint ratio": {
                "description": "Ratio between greenhouse cover and footprint areas (unitless)",
                "type": "decimal"
            },
            "mean height": {
                "description": "Mean greenhouse height (ft)",
                "type": "decimal"
            },
            "dli required": {
                "description": "DLI required by crops (mol / m2 d)",
                "type": "decimal"
            },
            "dli provided": {
                "description": "DLI provided by lighting system (mol / m2 d)",
                "type": "decimal"
            },
            "lai": {
                "description": "Crop LAI",
                "type": "decimal"
            },
            "r_b": {
                "description": "Crop r_b",
                "type": "decimal"
            },
            "r_s_min": {
                "description": "Crop r_s_min",
                "type": "decimal"
            },
            "r_s_min_night": {
                "description": "Crop r_s_min_night",
                "type": "decimal"
            },
            "l_s": {
                "description": "Crop l_s",
                "type": "decimal"
            },
            "l_i": {
                "description": "Crop l_i",
                "type": "decimal"
            },
            "min crop temperature": {
                "description": "Minimum acceptable crop temperature (C)",
                "type": "decimal"
            },
            "max crop temperature": {
                "description": "Maximum acceptable crop temperature (C)",
                "type": "decimal"
            },
            "crop optimal cycle": {
                "description": "Optimal crop cycle duration (d)",
                "type": "decimal"
            },
            "crop degree days per dev cycle": {
                "description": "Optimal amount of degree days per dev cycle (C d / cycle)",
                "type": "decimal"
            },
            "crop optimal ppfd": {
                "description": "Crop optimal PPFD (umol / m2 s)",
                "type": "decimal"
            },
            "crop max unlimited assimilation": {
                "description": "Crop maximum unlimited assimilation (mg / m2 s)",
                "type": "decimal"
            },
            "transmissivity": {
                "description": "Covers transmissivity (tau)",
                "digits": 3,
                "type": "decimal"
            },
            "reflectivity": {
                "description": "Covers reflectivity (p)",
                "digits": 2,
                "type": "decimal"
            },
            "utr": {
                "description": "Covers U_value (W/m2 K)",
                "digits": 2,
                "type": "decimal"
            },
            "lighting power density": {
                "description": "Required lighting power density (kW / sqft)",
                "type": "decimal"
            },
            "max allowable temperature": {
                "description": "Maximum allowable greenhouse temperature (C)",
                "default": 26.67,
                "type": "decimal"
            },
            "min allowable temperature": {
                "description": "Minimum allowable greenhouse temperature (C)",
                "default": 18.33,
                "type": "decimal"
            },
            "max allowable rh": {
                "description": "Maximum allowable RH (%)",
                "default": 0.8,
                "type": "decimal"
            },
            "is heating": {
                "description": "Whether to use a heating system",
                "type": "bool"
            },
            "is lighting": {
                "description": "Whether to use supplementary lighting",
                "type": "bool"
            },
            "is co2 injection": {
                "description": "Whether to use CO2 injection",
                "type": "bool"
            },
            "epsilon crop": {
                "description": "epsilon crop",
                "default": 3.0,
                "type": "decimal"
            },
            "vpd (day)": {
                "description": "VPD (day) (kPa)",
                "default": 0.8,
                "type": "decimal"
            },
            "vpd (night)": {
                "description": "VPD (night) (kPa)",
                "default": 0.5,
                "type": "decimal"
            },
            "air density": {
                "description": "Air density (kg / m3)",
                "default": 1.2,
                "type": "decimal"
            },
            "specific heat air": {
                "description": "Specific heat capacity of air (J / kg)",
                "default": 1000,
                "type": "decimal"
            },
            "latent heat of vapor": {
                "description": "Latent heat of vapor (J / kg)",
                "default": 2500,
                "type": "decimal"
            },
            "air-soil transfer coefficient": {
                "description": "Air-soil transfer coefficient",
                "default": 8.74,
                "type": "decimal"
            },
            "co2 ambient": {
                "description": "Ambient concentration of CO2 (ppm)",
                "default": 410,
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
            "scenario": {
                "description": "Selected scenario ID",
                "type": "integer"
            },
            "yield reduction suboptimal lighting": {
                "description": "Yield reduction due to suboptimal lighting (%)",
                "digits": 4,
                "type": "decimal"
            },
            "yield reduction suboptimal temperature": {
                "description": "Yield reduction due to suboptimal temperature (%)",
                "digits": 4,
                "type": "decimal"
            },
            "yield increase co2 injection": {
                "description": "Yield increase due to CO2 injection (%)",
                "digits": 4,
                "type": "decimal"
            },
            "yield impact coefficient": {
                "description": "Yield impact coefficient (unitless)",
                "digits": 4,
                "type": "decimal"
            },
            "energy use heating": {
                "description": "Total energy used for heating (kWh / y)",
                "digits": 2,
                "type": "decimal"
            },
            "heating system size": {
                "description": "Size of heating system (kW)",
                "digits": 2,
                "type": "decimal"
            },
            "water transpired": {
                "description": "Amount of water transpired yearly (L / y)",
                "digits": 1,
                "type": "decimal"
            },
            "ventilation total cfm": {
                "description": "Total ventilation rate in cubic feet per minute (CFM)",
                "digits": 0,
                "type": "decimal"
            },
            "ventilation time": {
                "description": "Time ventilation is active in hours per year (h / y)",
                "digits": 1,
                "type": "decimal"
            },
            "lighting time": {
                "description": "Hours of lighting required per year (h / y)",
                "digits": 1,
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
        inputs = parse_inputs(Gh_Energy.input_type(), args)

        output_type = Gh_Energy.output_type()
        result['sql_runtime'] = 0
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        from annoy import AnnoyIndex
        TREE_FILENAME_TEMPERATURE = "files/latitude-longitude-temperature.ann"
        TREE_FILENAME_IRRADIANCE = "files/latitude-longitude-irradiance.ann"
        QUERY_FILENAME = "files/gh_energy.sql"
        NUM_FEATURES = 2
        MAX_NEIGHBORS = 4

        def load_trees(tree_filename):
            """Loads the ANN file into the data structure"""
            u = AnnoyIndex(NUM_FEATURES, 'euclidean')
            u.load(tree_filename)
            return u

        def get_points(argument, filename):
            if argument not in globals():
                globals()[argument] = load_trees(filename)
            loaded_trees = globals()[argument]
            latitude = float(inputs['latitude'])
            longitude = float(inputs['longitude'])
            nn, distances = loaded_trees.get_nns_by_vector(
                (latitude, longitude), MAX_NEIGHBORS, include_distances=True)
            results = []
            total_weight = 0
            for i, d in zip(nn, distances):
                v = loaded_trees.get_item_vector(i)
                if d == 0:
                    # Perfect match, return only one location
                    return [{
                            'latitude': int(v[0]),
                            'longitude': int(v[1]),
                            'w': 1
                            }]
                total_weight += 1/d
                results.append(
                    {'latitude': int(v[0]), 'longitude': int(v[1]), 'w': 1/d})
            # Normalize location weights
            for k in results:
                k['w'] = k['w'] / total_weight
            return results

        def weight_point_clause(p):
            return 'when latitude = ' + str(int(p['latitude'])) + ' and longitude = ' + str(
                int(p['longitude'])) + ' then ' + str(p['w']) + ' '

        def weight_clause(points):
            return '(case ' + ''.join([weight_point_clause(p)
                                       for p in points]) + ' else null end)'

        def where_point_clause(p):
            return '(latitude = ' + str(int(p['latitude'])) + \
                ' and longitude = ' + str(int(p['longitude'])) + ')'

        def where_clause(points):
            return ' or '.join([where_point_clause(p) for p in points])

        def get_sql(points_temperature, points_irradiance):
            # Memoize file so that it's only loaded once
            if 'sql_files' not in globals():
                globals()['sql_files'] = {}
            sql_files = globals()['sql_files']
            if QUERY_FILENAME not in sql_files:
                with open(QUERY_FILENAME, 'r') as file:
                    sql_files[QUERY_FILENAME] = file.read()
            sql_template = sql_files[QUERY_FILENAME]
            return sql_template.replace('%%WEIGHT_CLAUSE_A%%', weight_clause(points_temperature)).replace('%%WHERE_CLAUSE_A%%', where_clause(
                points_temperature)).replace('%%WEIGHT_CLAUSE_B%%', weight_clause(points_irradiance)).replace('%%WHERE_CLAUSE_B%%', where_clause(points_irradiance))
        points_temperature = get_points(
            'loaded_trees_temperature',
            TREE_FILENAME_TEMPERATURE)
        points_irradiance = get_points(
            'loaded_trees_irradiance',
            TREE_FILENAME_IRRADIANCE)
        sql = get_sql(points_temperature, points_irradiance)
        query_inputs = {
            # Crop parameters
            'dli_required': round(inputs['dli required'], 4),
            'dli_provided': round(inputs['dli provided'], 4),
            'LAI': round(inputs['lai'], 4),
            'r_b': round(inputs['r_b'], 4),
            'r_s_min': round(inputs['r_s_min'], 4),
            'r_s_min_night': round(inputs['r_s_min_night'], 4),
            'l_s': round(inputs['l_s'], 4),
            'l_i': round(inputs['l_i'], 4),
            'min_crop_temperature': round(inputs['min crop temperature'], 2),
            'max_crop_temperature': round(inputs['max crop temperature'], 2),
            'optimal_avg_PPFD__umol_m2_s': round(inputs['crop optimal ppfd'], 2),
            'max_unlimited_assimilation__mg_m2_s': round(inputs['crop max unlimited assimilation'], 2),
            'light_sum': round(inputs['crop optimal cycle'] * inputs['dli required'], 4),
            'cycles_per_year': round(365 / inputs['crop optimal cycle'], 4),
            'degree_days_per_dev_cycle': round(inputs['crop degree days per dev cycle'], 4),
            # Equipment parameters
            'transmissivity': round(inputs['transmissivity'] * 0.9, 6),
            'reflectivity': round(inputs['reflectivity'], 6),
            'utr': round(inputs['utr'], 6),
            'kW_per_sqft': round(inputs['lighting power density'], 8),
            # GH dimension parameters
            'Ac_As': round(inputs['cover to footprint ratio'], 6),
            'mean_height_m2': round(inputs['mean height'] * 0.3048, 4),
            'total_area_m2': round(inputs['area'] * 0.092903, 4),
            'total_volume_cuft': round(inputs['volume'], 4),
            # Ventilation options
            'ventilation_rate_low_wind': 10,
            'ventilation_rate_high_wind': 20,
            'ventilation_rate_forced_a': 45,
            'ventilation_rate_forced_b': 60,
            # Control parameters
            'max_allowable_temp': round(inputs['max allowable temperature'], 2),
            'min_allowable_temp': round(inputs['min allowable temperature'], 2),
            'max_allowable_RH': round(inputs['max allowable rh'], 4),
            'is_heating': inputs['is heating'],
            'is_lighting': inputs['is lighting'],
            'co2_injection_rate': 5 if inputs['is co2 injection'] else 0,
            # Atmospheric parameters
            'epsilon_crop': round(inputs['epsilon crop'], 1),
            'vpd_day': round(inputs['vpd (day)'], 4),
            'vpd_night': round(inputs['vpd (night)'], 4),
            'air_density': round(inputs['air density'], 4),
            'specific_heat_air': round(inputs['specific heat air'], 4),
            'c_heat_vapor': round(inputs['latent heat of vapor'], 4),
            'air_soil_transfer_coefficient': round(inputs['air-soil transfer coefficient'], 4),
            'co2_ambient_ppm': round(inputs['co2 ambient'], 4)
        }
        print('energy_query_inputs', query_inputs)
        s_start = timeit.default_timer()
        result_row = fetch_one(sql, query_inputs, dbname='location')
        result['sql_runtime'] += timeit.default_timer() - s_start
        set_if_unset(result, "scenario", result_row["scenario"])
        set_if_unset(result, "yield reduction suboptimal lighting",
                     result_row["reduction_suboptimal_light_yield"])
        set_if_unset(
            result,
            "yield reduction suboptimal temperature",
            result_row["reduction_suboptimal_temperature"])
        set_if_unset(
            result,
            "yield increase co2 injection",
            result_row["increase_co2_yield"])
        set_if_unset(
            result,
            "yield impact coefficient",
            result_row["yield_coefficient"])
        set_if_unset(
            result,
            "energy use heating",
            result_row["total_energy_use_heating_kwh"])
        set_if_unset(
            result,
            "heating system size",
            result_row["heating_system_size_kw"])
        set_if_unset(
            result,
            "water transpired",
            result_row["water_transpired_l"])
        set_if_unset(
            result,
            "ventilation total cfm",
            result_row["ventilation_cfm"])
        set_if_unset(
            result,
            "ventilation time",
            result_row["ventilation_hours"])
        set_if_unset(
            result,
            "lighting time",
            result_row["light_hours_per_year"] if inputs['is lighting'] == True else 0)
        result['runtime'] = timeit.default_timer() - start
        if quantize_output:
            quantize_outputs(output_type, result)

        return result
