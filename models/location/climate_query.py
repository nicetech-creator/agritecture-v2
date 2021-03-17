from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math

class Climate_Query:

    @staticmethod
    def info():
        return {
            "name": "climate_query",
            "description": "Location query utility"
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
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def input_values(input_name):
        raise ValueError("Unexpected input: " + str(input_name))

    @staticmethod
    def output_type():
        return {
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def compute(args, quantize_output=False):
        result = dict()
        inputs = parse_inputs(Climate_Query.input_type(), args)

        output_type = Climate_Query.output_type()
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        latitude = inputs['latitude']
        longitude = inputs['longitude']

        sql_template = """
            with data_source_temperature as (
            select
                datetime,
                cast(sum(w * Dry_bulb_temperature__deg__C_) / sum(w) as double precision) T_out__C_,
                cast(sum(w * Relative_Humidity____) / sum(w) as double precision) RH_out,
                cast(sum(w * Windspeed__m_s_) / sum(w) as double precision) Windspeed__m_s,
                cast(sum(w * Wind_direction__deg_) / sum(w) as double precision) Wind_direction__deg
            from (
                select
                    %%WEIGHT_CLAUSE_A%% w, datetime, Dry_bulb_temperature__deg__C_, Relative_Humidity____, Windspeed__m_s_, Wind_direction__deg_
                from
                    tmy
                where
                    %%WHERE_CLAUSE_A%%
            ) a
            group by 1
            ),
            data_source_irradiance as (
            select
                datetime,
                cast(sum(w * Global_Horizontal_Irradiance__W_m2_) / sum(w) as double precision) I_glob__W_m2_
            from (
                select
                    %%WEIGHT_CLAUSE_B%% w, datetime, Global_Horizontal_Irradiance__W_m2_
                from
                    tmy
                where
                    %%WHERE_CLAUSE_B%%
            ) a
            group by 1
            )
            select
                  t.datetime, t.T_out__C_, t.RH_out, i.I_glob__W_m2_, t.Windspeed__m_s, t.Wind_direction__deg
            from
                data_source_temperature t
                left join data_source_irradiance i on (t.datetime = i.datetime)
            order by datetime
            """
        from annoy import AnnoyIndex
        TREE_FILENAME_TEMPERATURE = "files/latitude-longitude-temperature.ann"
        TREE_FILENAME_IRRADIANCE = "files/latitude-longitude-irradiance.ann"
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
            return sql_template.replace('%%WEIGHT_CLAUSE_A%%', weight_clause(points_temperature)).replace('%%WHERE_CLAUSE_A%%', where_clause(
                points_temperature)).replace('%%WEIGHT_CLAUSE_B%%', weight_clause(points_irradiance)).replace('%%WHERE_CLAUSE_B%%', where_clause(points_irradiance))
        points_temperature = get_points(
            'loaded_trees_temperature',
            TREE_FILENAME_TEMPERATURE)
        points_irradiance = get_points(
            'loaded_trees_irradiance',
            TREE_FILENAME_IRRADIANCE)
        sql = get_sql(points_temperature, points_irradiance)
        result_rows = fetch_all(sql, dbname='location')
        data = []
        
        for line in result_rows:
            print (line)
            data.append({
                'datetime': line['datetime'].strftime("%m/%d/%Y %H:%M:%S"),
                'temperature': line['t_out__c_'],
                'humidity': line['rh_out'],
                'irridiance': line['i_glob__w_m2_'],
                'windspeed': line['windspeed__m_s'],
                'wind_direction': line['wind_direction__deg']
            })
        return data
