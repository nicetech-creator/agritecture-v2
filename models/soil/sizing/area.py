from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math


class Soil_Sizing_Area:

    @staticmethod
    def info():
        return {
            "name": "soil_sizing_area",
            "description": "Area allocation for Soil"
        }

    @staticmethod
    def input_type():
        return {
            "area": {
                "description": "Total area to be allocated (sqft)",
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
            "overall": {
                "type": "list",
                "description": "Overall area allocation",
                "element type": {
                    "description": {
                        "type": "text",
                        "description": "Item description"
                    },
                    "percentage": {
                        "description": "Fraction of area allocated (%)",
                        "digits": 4,
                        "type": "decimal"
                    },
                    "area": {
                        "description": "Area allocated (sqft)",
                        "digits": 0,
                        "type": "decimal"
                    }
                }
            },
            "main bedspace": {
                "description": "Available bedspace (sqft)",
                "digits": 0,
                "type": "decimal"
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def compute(args, quantize_output=False):
        result = dict()
        inputs = parse_inputs(Soil_Sizing_Area.input_type(), args)

        output_type = Soil_Sizing_Area.output_type()
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        result['overall'] = [{
            'description': 'Cultivation',
            'percentage': 0.56
        }, {
            'description': 'Row aisles',
            'percentage': 0.24
        }, {
            'description': 'Processing',
            'percentage': 0.05
        }, {
            'description': 'Nursery',
            'percentage': 0.05
        }, {
            'description': 'Storage',
            'percentage': 0.1
        }, {
            'description': 'Total',
            'percentage': 1
        }]
        for line in result['overall']:
            line['area'] = inputs['area'] * line['percentage']
        set_if_unset(result, 'main bedspace', result['overall'][0]['area'])

        if quantize_output:
            quantize_outputs(output_type, result)

        return result
