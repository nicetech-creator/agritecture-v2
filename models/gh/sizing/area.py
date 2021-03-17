from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math
import sys
import timeit
class Gh_Sizing_Area:

    @staticmethod
    def info():
        return {
            "name": "gh_sizing_area",
            "description": "Area allocation for GH"
        }

    @staticmethod
    def input_type():
        return {
            "area": {
                "description": "Total area to be allocated (sqft)",
                "type": "decimal"
            },
            "bench width": {
                "description": "Bench width dimension (ft)",
                "default": 4,
                "type": "decimal"
            },
            "bench length": {
                "description": "Bench length dimension (ft)",
                "default": 10,
                "type": "decimal"
            },
            "headhouse_enabled": {
                "description": "Enable/disable headhouse setting",
                "default": True,
                "type": "bool"
            },
            "circulation_enabled": {
                "description": "Enable/disable circulation setting",
                "default": True,
                "type": "bool"
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
            "headhouse": {
                "type": "list",
                "description": "Headhouse area allocation",
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
            "bays": {
                "type": "list",
                "description": "Bays area allocation",
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
            "main allocated": {
                "description": "Main area allocated (sqft)",
                "digits": 0,
                "type": "decimal"
            },
            "main footprint per bench": {
                "description": "Footprint per bench (sqft)",
                "digits": 0,
                "type": "decimal"
            },
            "main number of benches": {
                "description": "Number of benches",
                "digits": 0,
                "type": "decimal"
            },
            "main bedspace": {
                "description": "Available bedspace (sqft)",
                "digits": 0,
                "type": "decimal"
            },
            "bays area": {
                "description": "Area allocated to bays (sqft)",
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
        inputs = parse_inputs(Gh_Sizing_Area.input_type(), args)

        output_type = Gh_Sizing_Area.output_type()
        result['sql_runtime'] = 0
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        # set headhouse_fraction according to site area
        if inputs['headhouse_enabled']:
            s_start = timeit.default_timer()
            size_factors = fetch_all('select area, headhouse from gh_headhouse_size')
            result['sql_runtime'] += timeit.default_timer() - s_start
            size = sys.maxsize
            factor = 0
            inf_factor = 0

            for row in size_factors:
                if row['area'] == 'inf': inf_factor = row['headhouse']
                else:
                    s = int(row['area'])
                    if inputs['area'] <= s and s <= size:
                        size = s
                        factor = row['headhouse']
            if size == sys.maxsize:
                factor = inf_factor
            headhouse_fraction = factor / 100
        else: headhouse_fraction = 0
        circulation_space_fraction = 0.15 if inputs['circulation_enabled'] else 0
        bays_fraction = 1 - headhouse_fraction - circulation_space_fraction

        result['overall'] = [{
            'description': 'Circulation space',
            'percentage': circulation_space_fraction
        }, {
            'description': 'Headhouse',
            'percentage': headhouse_fraction
        }, {
            'description': 'Bays',
            'percentage': bays_fraction
        }, {
            'description': 'Total',
            'percentage': 1
        }]
        for line in result['overall']:
            line['area'] = inputs['area'] * line['percentage']
        result['headhouse'] = [{
            'description': 'Seeding',
            'percentage': 0.25
        }, {
            'description': 'Processing',
            'percentage': 0.4
        }, {
            'description': 'Cold Storage',
            'percentage': 0.05
        }, {
            'description': 'Walkways',
            'percentage': 0.3
        }, {
            'description': 'Total',
            'percentage': 1
        }]
        area_headhouse = result['overall'][1]['area']
        for line in result['headhouse']:
            line['area'] = area_headhouse * line['percentage']
        result['bays'] = [{
            'description': 'Main System',
            'percentage': 0.6
        }, {
            'description': 'Nursery',
            'percentage': 0.1
        }, {
            'description': 'Walkways',
            'percentage': 0.3
        }, {
            'description': 'Total',
            'percentage': 1
        }]
        area_bays = result['overall'][2]['area']
        for line in result['bays']:
            line['area'] = area_bays * line['percentage']
        set_if_unset(result, 'main allocated', result['bays'][0]['area'])

        set_if_unset(
            result,
            "main footprint per bench",
            inputs["bench width"] *
            inputs["bench length"])
        set_if_unset(
            result,
            "main number of benches",
            math.floor(
                result["main allocated"] /
                result["main footprint per bench"]))
        set_if_unset(
            result,
            "main bedspace",
            result["main footprint per bench"] *
            result["main number of benches"])
        set_if_unset(result, "bays area", area_bays)
        result['runtime'] = timeit.default_timer() - start
        if quantize_output:
            quantize_outputs(output_type, result)

        return result
