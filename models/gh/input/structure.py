from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math
import timeit

class Gh_Input_Structure:

    @staticmethod
    def info():
        return {
            "name": "gh_input_structure",
            "description": "Input translator for GH structure"
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
            "structure type": {
                "type": "choice",
                "description": "Structure type",
                "default": 1
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def input_values(input_name):
        if input_name == "structure type":
            return [{'id': 1, 'description': 'Heavy duty'}, {
                'id': 2, 'description': 'Medium duty'}, {'id': 3, 'description': 'Light duty'}]
        raise ValueError("Unexpected input: " + str(input_name))

    @staticmethod
    def output_type():
        return {
            "structure element_id": {
                "description": "Selected ID for structure type",
                "type": "integer"
            },
            "covers element_id": {
                "description": "Selected ID for cover type",
                "type": "integer"
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
        inputs = parse_inputs(Gh_Input_Structure.input_type(), args)

        output_type = Gh_Input_Structure.output_type()
        result['sql_runtime'] = 0
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        from annoy import AnnoyIndex
        TREE_FILENAME_TEMPERATURE = "files/latitude-longitude-temperature.ann"
        NUM_FEATURES = 2
        MAX_NEIGHBORS = 4

        def load_trees(tree_filename):
            """Loads the ANN file into the data structure"""
            u = AnnoyIndex(NUM_FEATURES, 'euclidean')
            u.load(tree_filename)
            return u

        def get_points():
            if 'loaded_trees_temperature' not in globals():
                globals()['loaded_trees_temperature'] = load_trees(
                    TREE_FILENAME_TEMPERATURE)
            loaded_trees_temperature = globals()['loaded_trees_temperature']
            latitude = float(inputs['latitude'])
            longitude = float(inputs['longitude'])
            nn, distances = loaded_trees_temperature.get_nns_by_vector(
                (latitude, longitude), MAX_NEIGHBORS, include_distances=True)
            results = []
            total_weight = 0
            for i, d in zip(nn, distances):
                v = loaded_trees_temperature.get_item_vector(i)
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

        def get_sql(points):
            sql_template = '''
                with data_source as (
                    select
                        datetime,
                        sum(w * Dry_bulb_temperature__deg__C_) / sum(w) T_out__C_
                    from (
                        select
                            %%WEIGHT_CLAUSE%% w, *
                            from
                                tmy
                            where
                                %%WHERE_CLAUSE%%
                        ) a
                    group by 1
                )
                select min(T_out__C_) t from data_source where extract(hour from datetime) >= 8 and extract(hour from datetime) < 17
            '''
            return sql_template.replace('%%WEIGHT_CLAUSE%%', weight_clause(
                points)).replace('%%WHERE_CLAUSE%%', where_clause(points))

        def calculate_day_outside_temperature():
            points = get_points()
            sql = get_sql(points)
            row = fetch_one(sql, dbname='location')
            return row['t']

        if inputs['structure type'] == 1:
            structure_id = 4
            covers_id = 4
        elif inputs['structure type'] == 2:
            structure_id = 3
            covers_id = 3
        else:
            # Must look at outside temperature to decide
            s_start = timeit.default_timer()
            min_outside_day_temperature = calculate_day_outside_temperature()
            result['sql_runtime'] += timeit.default_timer() - s_start
            # Compare to 40 F
            if min_outside_day_temperature < (40 - 32) * 5 / 9:
                structure_id = 2
                covers_id = 2
            else:
                structure_id = 1
                covers_id = 1

        set_if_unset(result, "structure element_id", structure_id)
        set_if_unset(result, "covers element_id", covers_id)
        result['runtime'] = timeit.default_timer() - start
        if quantize_output:
            quantize_outputs(output_type, result)

        return result
