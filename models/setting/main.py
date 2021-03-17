from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math

class AD_Setting:
    @staticmethod
    def info():
        return {
            "name": "ad_setting",
            "description": "All constant values for AD engine"
        }
    
    @staticmethod
    def input_type():
        return {}
    
    @staticmethod
    def output_type():
        return {
            "vf_level_spacing":{
                "type": "list",
                "description": "Space setting for difference crops in VF",
                "element type": {
                    "name": {
                        "type": "text",
                        "description": "Condition name"
                    },
                    "space": {
                        "digits": 2,
                        "description": "Space between levels",
                        "type": "decimal"
                    }
                }
            },
            "gh_headhouse_size": {
                "type": "list",
                "description": "Headhouse space according to site area",
                "element type": {
                    "area": {
                        "type": "text",
                        "description": "Site Area Up to"
                    },
                    "headhouse": {
                        "digits": 2,
                        "description": "percentage of headhouse size",
                        "type": "decimal"
                    }
                }
            },
            "operation_types": {
                "type": "list",
                "description": "Available types with available grow types",
                "element type": {
                    "name": {
                        "type": "text",
                        "description": "Site Area Up to"
                    },
                    "nft": {
                        "description": "nft",
                        "type": "decimal"
                    },
                    "ebb_flow": {
                        "description": "ebb_flow",
                        "type": "decimal"
                    },
                    "deep_water_culture": {
                        "description": "deep_water_culture",
                        "type": "decimal"
                    },
                    "bucket": {
                        "description": "bucket",
                        "type": "decimal"
                    },
                    "slab": {
                        "description": "slab",
                        "type": "decimal"
                    },
                    "soil": {
                        "description": "soil",
                        "type": "decimal"
                    },
                    "aeroponic": {
                        "description": "aeroponic",
                        "type": "decimal"
                    }
                }
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def compute(args, quantize_output=False):
        result = dict()
        inputs = parse_inputs(AD_Setting.input_type(), args)

        output_type = AD_Setting.output_type()

        result['vf_level_spacing'] = []
        # read spacing setting from database
        rows = fetch_all('select name, value from vf_level_spacing')
        for row in rows:
            result['vf_level_spacing'].append({
                'name': row['name'],
                'space': row['value']
            })

        result['gh_headhouse_size'] = []
        # read spacing setting from database
        rows = fetch_all('select area, headhouse from gh_headhouse_size')
        for row in rows:
            result['gh_headhouse_size'].append({
                'area': row['area'],
                'headhouse': row['headhouse']
            })
        # read operation type from database
        result['operation_types'] = []
        rows = fetch_all('select * from operation_types')
        for row in rows:
            result['operation_types'].append({
                'name': row['name'],
                'nft': row['nft'],
                'ebb_flow': row['Ebb & Flow'],
                'deep_water_culture': row['dwc'],
                'bucket': row['bucket'],
                'slab': row['slab'],
                'soil': row['soil'],
                'aeroponic': row['aeroponics']
            })
        if quantize_output:
            quantize_outputs(output_type, result)

        return result