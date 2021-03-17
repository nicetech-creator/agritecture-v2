from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math
import timeit

class Vf_Sizing_Area:

    @staticmethod
    def info():
        return {
            "name": "vf_sizing_area",
            "description": "Area allocation for VF"
        }

    @staticmethod
    def input_type():
        return {
            "area": {
                "description": "Total area to be allocated (sqft)",
                "type": "decimal"
            },
            "height": {
                "description": "Available height for building (ft)",
                "type": "decimal"
            },
            "desired number of levels": {
                "description": "Desired number of levels",
                "type": "decimal"
            },
            "racking automation": {
                "description": "Whether automation is used in racking",
                "type": "bool",
                "default": True
            },
            "distance first level from floor": {
                "description": "Rack distance between first level and roof (ft)",
                "default": 2,
                "type": "decimal"
            },
            "distance last level from roof": {
                "description": "Rack distance between last level and roof (ft)",
                "default": 2.5,
                "type": "decimal"
            },
            "height between levels": {
                "description": "Rack height between levels (ft)",
                "default": 2,
                "type": "decimal"
            },
            "bench width": {
                "description": "Bench width dimension (ft)",
                "default": 4.33,
                "type": "decimal"
            },
            "bench length": {
                "description": "Bench length dimension (ft)",
                "default": 40,
                "type": "decimal"
            },
            "bench internal width": {
                "description": "Bench internal width dimension (ft)",
                "default": 4,
                "type": "decimal"
            },
            "processing": {
                "description": "Enable/disable processing setting",
                "type": "bool",
                "default": True
            },
            "cold storage": {
                "description": "Enable/disable cold storage setting",
                "type": "bool",
                "default": True
            },
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
            "main system footprint": {
                "description": "Main system footprint area (ft)",
                "digits": 0,
                "type": "decimal"
            },
            "max number of levels": {
                "description": "Maximum number of levels allowed",
                "type": "integer"
            },
            "footprint per tower": {
                "description": "Footprint taken per tower (ft)",
                "digits": 2,
                "type": "decimal"
            },
            "number of towers": {
                "description": "Number of towers in system",
                "type": "integer"
            },
            "number of levels": {
                "description": "Actual number of levels in system",
                "type": "integer"
            },
            "bedspace per tower": {
                "description": "Bedspace made available per tower (sqft)",
                "digits": 0,
                "type": "decimal"
            },
            "total bedspace": {
                "description": "Total available bedspace (sqft)",
                "digits": 0,
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
        inputs = parse_inputs(Vf_Sizing_Area.input_type(), args)

        output_type = Vf_Sizing_Area.output_type()
        result['sql_runtime'] = 0
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        addition = 0 # addtion when user disable either/ both processing or/and cold storage
        add_walkway = 0 #fraction substracted from walkway & circulation
        if inputs['processing'] == False: 
            addition += 0.1 
            add_walkway += 0.015
        if inputs['cold storage'] == False:
            addition += 0.02 
            add_walkway += 0.015

        result['overall'] = [{
            'description': 'Main System Footprint',
            'percentage': (0.45 if inputs['racking automation'] else 0.3) + addition * 0.8 + add_walkway
        }, {
            'description': 'Nursery',
            'percentage': 0.15
        }, {
            'description': 'Seeding and Germination',
            'percentage': 0.1 + addition * 0.2
        }, {
            'description': 'Harvesting and Processing',
            'percentage': 0.1 if inputs['processing'] else 0
        }, {
            'description': 'Cold Storage',
            'percentage': 0.02 if inputs['cold storage'] else 0
        }, {
            'description': 'Biosecurity',
            'percentage': 0.05
        }, {
            'description': 'Walkways',
            'percentage': (0.13 if inputs['racking automation'] else 0.28) - add_walkway
        }, {
            'description': 'Total',
            'percentage': 1
        }]
        for line in result['overall']:
            line['area'] = inputs['area'] * line['percentage']
        main_system_footprint = result['overall'][0]['area']

        set_if_unset(result, "main system footprint", main_system_footprint)
        set_if_unset(
            result,
            "max number of levels",
            max(
                int(
                    math.floor(
                        (inputs["height"] -
                         inputs["distance first level from floor"] -
                            inputs["distance last level from roof"]) /
                        inputs["height between levels"])),
                0))
        set_if_unset(
            result,
            "footprint per tower",
            inputs["bench length"] *
            inputs["bench width"])
        set_if_unset(
            result, "number of towers", int(
                math.ceil(
                    result["main system footprint"] / result["footprint per tower"])))
        set_if_unset(result,
                     "number of levels",
                     min(int(math.floor(inputs["desired number of levels"])),
                         result["max number of levels"]))
        set_if_unset(
            result,
            "bedspace per tower",
            inputs["bench length"] *
            inputs["bench internal width"] *
            result["number of levels"])
        set_if_unset(
            result,
            "total bedspace",
            result["bedspace per tower"] *
            result["number of towers"])
        result['runtime'] = timeit.default_timer() - start
        if quantize_output:
            quantize_outputs(output_type, result)

        return result