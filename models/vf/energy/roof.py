from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math
import timeit

class Vf_Roof:

    @staticmethod
    def info():
        return {
            "name": "vf_roof",
            "description": "Roof envelope for VF"
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
            "building insulation": {
                "type": "choice",
                "description": "Building insulation type"
            },
            "building roof type": {
                "type": "choice",
                "description": "Building roof type"
            },
            "site area": {
                "description": "Total site area (sqft)",
                "type": "decimal"
            },
            "height": {
                "description": "Site height (sqft)",
                "type": "decimal"
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def input_values(input_name):
        if input_name == "building insulation":
            return [{'id': 1, 'description': 'Low insulation'}, {
                'id': 2, 'description': 'Mid insulation'}, {'id': 3, 'description': 'High insulation'}]
        if input_name == "building roof type":
            return [{'id': 1, 'description': 'Flat roof'},
                    {'id': 2, 'description': 'Pitched roof'}]
        raise ValueError("Unexpected input: " + str(input_name))

    @staticmethod
    def output_type():
        return {
            "cover area": {
                "description": "Total cover area (sqft)",
                "digits": 1,
                "type": "decimal"
            },
            "cover cost": {
                "description": "Total cover cost ($)",
                "digits": 2,
                "type": "decimal"
            },
            "U value walls": {
                "description": "U value for walls (W / m2 C)",
                "digits": 3,
                "type": "decimal"
            },
            "U value floor": {
                "description": "U value for floors (W / m2 C)",
                "digits": 3,
                "type": "decimal"
            },
            "U value roof": {
                "description": "U value for roof (W / m2 C)",
                "digits": 3,
                "type": "decimal"
            },
            "U value": {
                "description": "Global U value (W / m2 C)",
                "digits": 3,
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
        inputs = parse_inputs(Vf_Roof.input_type(), args)

        output_type = Vf_Roof.output_type()
        result['sql_runtime'] = 0
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        from annoy import AnnoyIndex
        TREE_FILENAME_TEMPERATURE = "files/latitude-longitude-temperature.ann"
        QUERY_FILENAME = "files/vf_day_degrees.sql"
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
        query_inputs = {
            'cdd_lower_bound_C': 10,
            'hdd_higher_bound_C': 18
        }
        s_start = timeit.default_timer()
        result_row = fetch_one(sql, query_inputs, dbname='location')
        result['sql_runtime'] += timeit.default_timer() - s_start
        cdd = result_row['cdd']
        hdd = result_row['hdd']

        sql_ashrae = '''
            select walls, floor, roof from (
                select
                    *,
                    case
                        when (%(cdd)s >= cdd_thermal_criteria_low or cdd_thermal_criteria_low is null) and (%(cdd)s < cdd_thermal_criteria_high or cdd_thermal_criteria_high is null) then 1
                        else 0
                    end
                    + case
                        when (%(hdd)s >= hdd_thermal_criteria_low or hdd_thermal_criteria_low is null) and (%(hdd)s < hdd_thermal_criteria_high or hdd_thermal_criteria_high is null) then 1
                        else 0
                    end total_score
                from vf_ashrae_zone_requirement
                where walls is not null and floor is not null and roof is not null
                order by total_score desc, id asc
                limit 1
            ) a
        '''
        query_inputs = {
            'cdd': cdd,
            'hdd': hdd
        }
        s_start = timeit.default_timer()
        ashrae_row = fetch_one(sql_ashrae, query_inputs)
        result['sql_runtime'] += timeit.default_timer() - s_start
        ashrae_req_walls = ashrae_row["walls"]
        ashrae_req_floor = ashrae_row["floor"]
        ashrae_req_roof = ashrae_row["roof"]
        from decimal import Decimal
        if inputs['building insulation'] == 1:
            insulation_value = Decimal('0.2')
        elif inputs['building insulation'] == 2:
            insulation_value = Decimal('0.5')
        else:
            insulation_value = Decimal('1')
        r_value_walls = ashrae_req_walls * (1 - insulation_value)
        r_value_floor = ashrae_req_floor * (1 - insulation_value)
        r_value_roof = ashrae_req_roof * (1 - insulation_value)
        u_walls = Decimal('5.6') / ashrae_req_walls
        u_floor = Decimal('5.6') / ashrae_req_floor
        u_roof = Decimal('5.6') / ashrae_req_roof
        sql_wall = "select total from vf_ashrae_walls where r_value >= %(r)s order by r_value asc limit 1"
        sql_floor_and_roof = "select total from vf_ashrae_floor_and_roof where r_value >= %(r)s order by r_value asc limit 1"
        s_start = timeit.default_timer()
        wall_cost = fetch_one(sql_wall, {'r': r_value_walls})['total']
        floor_cost = fetch_one(
            sql_floor_and_roof, {
                'r': r_value_floor})['total']
        roof_cost = fetch_one(sql_floor_and_roof, {'r': r_value_roof})['total']
        result['sql_runtime'] += timeit.default_timer() - s_start
        half_site_area_sqrt = math.sqrt(inputs["site area"] / 2)
        flat_roof_wall = (2*half_site_area_sqrt + 4 *
                          half_site_area_sqrt) * inputs["height"]
        flat_roof_floor = inputs["site area"]
        flat_roof_roof = flat_roof_floor
        flat_roof_area = flat_roof_wall + flat_roof_floor + flat_roof_roof
        flat_roof_cost = flat_roof_wall * wall_cost + \
            flat_roof_floor * floor_cost + flat_roof_roof * roof_cost
        pitched_roof_wall = flat_roof_wall
        pitched_roof_floor = flat_roof_floor
        pitched_roof_roof = flat_roof_roof * 1.08
        pitched_roof_area = pitched_roof_wall + pitched_roof_floor + pitched_roof_roof
        pitched_roof_cost = pitched_roof_wall * wall_cost + \
            pitched_roof_floor * floor_cost + pitched_roof_roof * roof_cost
        set_if_unset(
            result,
            "cover area",
            flat_roof_area if inputs["building roof type"] == 1 else pitched_roof_area)
        set_if_unset(
            result,
            "cover cost",
            flat_roof_cost if inputs["building roof type"] == 1 else pitched_roof_cost)
        set_if_unset(result, "U value walls", u_walls)
        set_if_unset(result, "U value floor", u_floor)
        set_if_unset(result, "U value roof", u_roof)
        set_if_unset(result, "U value", u_walls + u_floor + u_roof)

        result['runtime'] = timeit.default_timer() - start
        if quantize_output:
            quantize_outputs(output_type, result)

        return result
