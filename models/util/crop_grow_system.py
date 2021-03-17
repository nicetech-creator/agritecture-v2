from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math


class Crop_Grow_System:

    @staticmethod
    def info():
        return {
            "name": "crop_grow_system",
            "description": "Gets list of valid grow system types for a given crop"
        }

    @staticmethod
    def input_type():
        return {
            "crop id": {
                "type": "choice",
                "description": "Selected crop item"
                },
            "operation_type":{
                "type": "string",
                "default": "Greenhouse"
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def input_values(input_name):
        if input_name == "crop id":
            return fetch_all("SELECT id, crop_type FROM crops ORDER BY id")
        raise ValueError("Unexpected input: " + str(input_name))

    @staticmethod
    def output_type():
        return {
            "grow systems": {
                "type": "list",
                "description": "Grow systems available for the crop",
                "element type": {
                    "id": {
                        "type": "integer",
                        "description": "Grow system ID",
                        "primary key": True
                    },
                    "description": {
                        "type": "text",
                        "description": "Grow system description"
                    }
                }
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def compute(args, quantize_output=False):
        result = dict()
        inputs = parse_inputs(Crop_Grow_System.input_type(), args)

        output_type = Crop_Grow_System.output_type()
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        sql = '''
            with crop_data as (
                    select nft, ebb_flow, deep_water_culture, bucket, slab, soil, aeroponic from crops where id = %(crop_id)s
            )
            select g.id, g.description from (
                    select (case when count(*) = 0 then null else 1 end) id from crop_data where nft = 1
                    union all
                    select (case when count(*) = 0 then null else 2 end) id from crop_data where ebb_flow = 1
                    union all
                    select (case when count(*) = 0 then null else 3 end) id from crop_data where deep_water_culture = 1
                    union all
                    select (case when count(*) = 0 then null else 4 end) id from crop_data where bucket = 1
                    union all
                    select (case when count(*) = 0 then null else 5 end) id from crop_data where slab = 1
                    union all
                    select (case when count(*) = 0 then null else 6 end) id from crop_data where soil = 1
                    union all
                    select (case when count(*) = 0 then null else 7 end) id from crop_data where aeroponic = 1
            ) k
            join grow_system_yield g on k.id = g.id
            where k.id is not null
        '''
        query_inputs = {
            'crop_id': inputs['crop id']
        }
        result['grow systems'] = [
            {'id': row['id'], 'description': row['description']} for row in fetch_all(sql, query_inputs)]

         # filter out ones not suitable for operation type
        sql = "select * from operation_types where name=%(operation_type)s"
        query_inputs = {
            'operation_type': inputs['operation_type']
        }
        available_systems = fetch_one(sql, query_inputs)

        grow_system_dic = {
            'NFT': 'nft',
            'Ebb/Flow': 'Ebb & Flow',
            'Deep Water Culture': 'dwc',
            'Bucket': 'bucket',
            'Slab': 'slab',
            'Aeroponic': 'aeroponics',
            'Soil': 'soil'
        }
        grow_systems = []
        for system in result['grow systems']:
            if available_systems[grow_system_dic[system['description']]] == 1:
                grow_systems.append(system)
        result['grow systems'] = grow_systems

        if quantize_output:
            quantize_outputs(output_type, result)

        return result
