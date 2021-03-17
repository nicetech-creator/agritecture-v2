from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math
import timeit

class Vf_Energy:

    @staticmethod
    def info():
        return {
            "name": "vf_energy",
            "description": "Energy calculation for VF"
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
            "site area": {
                "description": "Total site area (sqft)",
                "type": "decimal"
            },
            "bedspace": {
                "description": "Available bedspace (sqft)",
                "type": "decimal"
            },
            "cover area": {
                "description": "Total cover area (sqft)",
                "type": "decimal"
            },
            "U value": {
                "description": "Global U value (W/m2 C)",
                "default": 0.733,
                "type": "decimal"
            },
            "photosynthetic efficacy": {
                "description": "Light system photosynthetic efficacy (umol / J)",
                "type": "decimal"
            },
            "light start hour": {
                "description": "Time lights are switched on",
                "default": 22,
                "type": "decimal"
            },
            "light end hour": {
                "description": "Time lights are switched off",
                "default": 8,
                "type": "decimal"
            },
            "inside day temperature": {
                "description": "Setpoint for inside day temperature (C)",
                "type": "decimal"
            },
            "inside night temperature": {
                "description": "Setpoint for inside night temperature (C)",
                "type": "decimal"
            },
            "inside day relative humidity": {
                "description": "Setpoint for inside day relative humidity (%)",
                "type": "decimal"
            },
            "inside night relative humidity": {
                "description": "Setpoint for inside night relative humidity (%)",
                "type": "decimal"
            },
            "crop optimal ppfd": {
                "description": "Crop optimal PPFD (umol / m2 s)",
                "type": "decimal"
            },
            "crop optimal dli": {
                "description": "Crop optimal dli",
                "type": "decimal"
            },
            "lai": {
                "description": "Crop LAI",
                "default": 3,
                "type": "decimal"
            },
            "r_b": {
                "description": "Crop r_b",
                "default": 200,
                "type": "decimal"
            },
            "epsilon crop": {
                "description": "epsilon crop",
                "default": 3.0,
                "type": "decimal"
            },
            "latent heat of vapor": {
                "description": "Latent heat of vapor (J / kg)",
                "default": 2500,
                "type": "decimal"
            },
            "PAR reflection coefficient": {
                "default": 0.05,
                "type": "decimal"
            },
            "cultivation area": {
                "default": 0.95,
                "type": "decimal"
            },
            "heat dissipation (lighting inefficiency)": {
                "default": 1,
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
            "water transpired": {
                "description": "Amount of water transpired yearly (gallons / y)",
                "digits": 1,
                "type": "decimal"
            },
            "provided light sum": {
                "description": "Yearly sum of DLI (mol / m2 d)",
                "digits": 1,
                "type": "decimal"
            },
            "tons of cooling": {
                "description": "Total cooling (tons of cooling)",
                "digits": 1,
                "type": "decimal"
            },
            "lighting power": {
                "description": "Required lighting power (kWh / y)",
                "digits": 2,
                "type": "decimal"
            },
            "kBTU cooling": {
                "description": "Required HVAC power (kBTU)",
                "digits": 4,
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
        inputs = parse_inputs(Vf_Energy.input_type(), args)

        output_type = Vf_Energy.output_type()
        result['sql_runtime'] = 0
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        # Pre-calculated constants for query
        PAR_flux_density = inputs["crop optimal ppfd"] / 5
        Rn_crop = (1 - inputs["PAR reflection coefficient"]) * \
            PAR_flux_density * inputs["cultivation area"]
        r_s = 60*((1500 + inputs["crop optimal ppfd"]) /
                  (200 + inputs["crop optimal ppfd"]))
        r_s_min_night = 2500
        day_temperature_C = inputs["inside day temperature"]
        night_temperature_C = inputs["inside night temperature"]
        humidity_setpoint_day = inputs["inside day relative humidity"]
        humidity_setpoint_night = inputs["inside night relative humidity"]
        vpd_day = (10**(2.7857+((7.5*day_temperature_C) /
                                (237.3+day_temperature_C)))/1000) * (1-humidity_setpoint_day)
        vpd_night = (10**(2.7857+((7.5*night_temperature_C)/(237.3 +
                                                             night_temperature_C)))/1000) * (1-humidity_setpoint_night)
        transp__g_m2_s_lights_on = (2 * inputs["lai"]/((1 + inputs["epsilon crop"]) * inputs["r_b"]+r_s))*(
            7.4*vpd_day+(inputs["epsilon crop"] * inputs["r_b"]*Rn_crop/(2 * inputs["lai"] * inputs["latent heat of vapor"])))
        transp__g_m2_s_lights_off = (2 * inputs["lai"]/((1 + inputs["epsilon crop"]) * inputs["r_b"]+r_s_min_night))*(
            7.4*vpd_night+(inputs["epsilon crop"] * inputs["r_b"]*Rn_crop/(2 * inputs["lai"] * inputs["latent heat of vapor"])))
        Qlights__W_m2_lights_on = (
            inputs["crop optimal ppfd"] / inputs["photosynthetic efficacy"]) * inputs["heat dissipation (lighting inefficiency)"]
        global_u_value = inputs["U value"]
        Qfacade__W_m2_multiplier = inputs["cover area"] * \
            global_u_value / inputs["site area"]
        from annoy import AnnoyIndex
        TREE_FILENAME_TEMPERATURE = "files/latitude-longitude-temperature.ann"
        QUERY_FILENAME = "files/vf_energy.sql"
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

        def get_sql(points_temperature):
            # Memoize file so that it's only loaded once
            if 'sql_files' not in globals():
                globals()['sql_files'] = {}
            sql_files = globals()['sql_files']
            if QUERY_FILENAME not in sql_files:
                with open(QUERY_FILENAME, 'r') as file:
                    sql_files[QUERY_FILENAME] = file.read()
            sql_template = sql_files[QUERY_FILENAME]
            return sql_template.replace('%%WEIGHT_CLAUSE%%', weight_clause(
                points_temperature)).replace('%%WHERE_CLAUSE%%', where_clause(points_temperature))
        points_temperature = get_points(
            'loaded_trees_temperature',
            TREE_FILENAME_TEMPERATURE)
        sql = get_sql(points_temperature)

        # set_if_unset(result, "lighting time", result_row["light_hours"])
        # new lighting time formual
        lighting_day = math.ceil(inputs["crop optimal dli"] / inputs["crop optimal ppfd"] * 278)
        set_if_unset(result, "lighting time", lighting_day * 365)

        #adjust input for lighting_on/off hour
        result['light end hour'] = (inputs['light start hour'] + lighting_day) % 24
        query_inputs = {
            # Physical parameters
            'c_heat_vapor': inputs['latent heat of vapor'],
            # Control parameters
            'hour_lights_on': inputs['light start hour'],
            'hour_lights_off': result['light end hour'],
            'day_temperature_C': day_temperature_C,
            'night_temperature_C': night_temperature_C,
            'required_ppfd': inputs['crop optimal ppfd'],
            # Structure constraints
            'total_bedspace_m2': inputs['bedspace'] * 0.092903,
            # Pre-calculated constants
            'transp__g_m2_s_lights_on': transp__g_m2_s_lights_on,
            'transp__g_m2_s_lights_off': transp__g_m2_s_lights_off,
            'Qlights__W_m2_lights_on': Qlights__W_m2_lights_on,
            'Qfacade__W_m2_multiplier': Qfacade__W_m2_multiplier
        }
        print('energy_query_inputs', query_inputs)
        s_start = timeit.default_timer()
        result_row = fetch_one(sql, query_inputs, dbname='location')
        result['sql_runtime'] += timeit.default_timer() - s_start
        
        set_if_unset(
            result,
            "provided light sum",
            result_row["provided_light_sum"])
        set_if_unset(result, "tons of cooling", result_row["tons_of_cooling"])
        set_if_unset(result, "lighting power", result_row["lighting_power"])
        set_if_unset(result, "kBTU cooling", result_row["kbtu"])
        set_if_unset(
            result,
            "water transpired",
            result_row["water_transpired"])
        
        result['runtime'] = timeit.default_timer() - start
        if quantize_output:
            quantize_outputs(output_type, result)

        return result
