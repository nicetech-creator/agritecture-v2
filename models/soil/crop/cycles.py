from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math


class Soil_Crop_Cycles:

    @staticmethod
    def info():
        return {
            "name": "soil_crop_cycles",
            "description": "Counts effective number of crop cycles for soil"
        }

    @staticmethod
    def input_type():
        return {
            "latitude": {
                "description": "Geographical latitude",
                "type": "decimal"
            },
            "longitude": {
                "description": "Geographical longitude",
                "type": "decimal"
            },
            "crops": {
                "type": "list",
                "description": "Selection of crops to compute",
                "element type": {
                    "id": {
                        "description": "Crop identifier",
                        "primary key": True,
                        "type": "text"
                    },
                    "absolute min temp": {
                        "description": "Absolute min temperature for crop (C)",
                        "type": "decimal"
                    },
                    "optimal min temp": {
                        "description": "Optimal min temperature for crop (C)",
                        "type": "decimal"
                    },
                    "optimal max temp": {
                        "description": "Optimal max temperature for crop (C)",
                        "type": "decimal"
                    },
                    "absolute max temp": {
                        "description": "Absolute max temperature for crop (C)",
                        "type": "decimal"
                    },
                    "optimal crop cycle": {
                        "description": "Duration of crop cycle in optimal temperature (days)",
                        "type": "decimal"
                    }
                }
            },
            "complementary crop absolute min temp": {
                "description": "Absolute min temperature for complementary crop (C)",
                "type": "decimal"
            },
            "complementary crop optimal min temp": {
                "description": "Optimal min temperature for complementary crop (C)",
                "type": "decimal"
            },
            "complementary crop optimal max temp": {
                "description": "Optimal max temperature for complementary crop (C)",
                "type": "decimal"
            },
            "complementary crop absolute max temp": {
                "description": "Absolute max temperature for complementary crop (C)",
                "type": "decimal"
            },
            "complementary crop optimal crop cycle": {
                "description": "Duration of crop cycle in optimal temperature for complementary crop (days)",
                "type": "decimal"
            },
            "suboptimal stretch factor": {
                "description": "Time dilatation factor for crop growth in suboptimal temperature (%)",
                "default": 0.7,
                "type": "decimal"
            },
            "cycle exchange period": {
                "description": "Number of optimal days used when switching between crop cycles (days)",
                "default": 3,
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
            "crops": {
                "type": "list",
                "description": "Cycle data information calculated per crop",
                "element type": {
                    "id": {
                        "description": "Crop identifier",
                        "primary key": True,
                        "type": "text"
                    },
                    "main crop cycles": {
                        "description": "Effective number of cycles on main crop",
                        "type": "integer"
                    },
                    "complementary crop cycles": {
                        "description": "Effective number of cycles on complementary crop",
                        "type": "integer"
                    }
                }
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def compute(args, quantize_output=False):
        result = dict()
        inputs = parse_inputs(Soil_Crop_Cycles.input_type(), args)

        output_type = Soil_Crop_Cycles.output_type()
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        from annoy import AnnoyIndex
        TREE_FILENAME_TEMPERATURE = "files/latitude-longitude-temperature.ann"
        TREE_FILENAME_IRRADIANCE = "files/latitude-longitude-irradiance.ann"
        QUERY_FILENAME = "files/soil_cycles.sql"
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

        def get_crop_data():
            crop_lines = []
            assert len(inputs['crops']) > 0
            for line in inputs['crops']:
                result_line = "select '%%CROP_TYPE%%' crop_type, %%ABSOLUTE_MIN_TEMP%% absolute_min_temp, %%OPTIMAL_MIN_TEMP%% optimal_min_temp, %%OPTIMAL_MAX_TEMP%% optimal_max_temp, %%ABSOLUTE_MAX_TEMP%% absolute_max_temp, %%OPTIMAL_CROP_CYCLE%% optimal_crop_cycle"
                result_line = result_line.replace(
                    "%%CROP_TYPE%%", str(line['id']))
                result_line = result_line.replace(
                    "%%ABSOLUTE_MIN_TEMP%%", str(
                        line['absolute min temp']))
                result_line = result_line.replace(
                    "%%OPTIMAL_MIN_TEMP%%", str(
                        line['optimal min temp']))
                result_line = result_line.replace(
                    "%%OPTIMAL_MAX_TEMP%%", str(
                        line['optimal max temp']))
                result_line = result_line.replace(
                    "%%ABSOLUTE_MAX_TEMP%%", str(
                        line['absolute max temp']))
                result_line = result_line.replace(
                    "%%OPTIMAL_CROP_CYCLE%%", str(
                        line['optimal crop cycle']))
                crop_lines.append(result_line)
            return ' union all '.join(crop_lines)
        sql = sql.replace("%%CROP_DATA%%", get_crop_data())
        query_inputs = {
            'cycle_exchange_period': str(inputs['cycle exchange period']),
            'suboptimal_stretch_factor': inputs['suboptimal stretch factor'],
            'complementary_crop_absolute_min_temp': inputs['complementary crop absolute min temp'],
            'complementary_crop_optimal_min_temp': inputs['complementary crop optimal min temp'],
            'complementary_crop_optimal_max_temp': inputs['complementary crop optimal max temp'],
            'complementary_crop_absolute_max_temp': inputs['complementary crop absolute max temp'],
            'complementary_crop_optimal_crop_cycle': inputs['complementary crop optimal crop cycle']
        }

        cycle_data = fetch_all(sql, query_args=query_inputs, dbname='location')
        result['crops'] = []
        for row in cycle_data:
            result['crops'].append({
                'id': row['crop_type'],
                'main crop cycles': row['num_cycles'],
                'complementary crop cycles': row['num_complementary_cycles']
            })

        if quantize_output:
            quantize_outputs(output_type, result)

        return result
