from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math
import timeit

class Vf_Grow:

    @staticmethod
    def info():
        return {
            "name": "vf_grow",
            "description": "Grow system selector for VF"
        }

    @staticmethod
    def input_type():
        return {
            "bedspace": {
                "description": "Bedspace area (sqft)",
                "type": "decimal"
            },
            "crops": {
                "type": "list",
                "description": "Selection of crops to compute",
                "element type": {
                    "id": {
                        "type": "choice",
                        "description": "Selected crop item",
                        "primary key": True
                    },
                    "system fraction": {
                        "description": "Fraction of bedspace allocated to crop",
                        "type": "decimal"
                    },
                    "sale unit id": {
                        "type": "choice",
                        "description": "Selected sale unit for crop item"
                    },
                    "price per unit": {
                        "description": "Sale price for crop per selected unit",
                        "type": "decimal"
                    },
                    "grow system type": {
                        "type": "choice",
                        "description": "Selection for grow system type",
                        "default": 1
                    }
                }
            },
            "depreciation duration": {
                "description": "Duration of item depreciation",
                "default": 15,
                "type": "decimal"
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def input_values(input_name):
        if input_name == "crops.id":
            return fetch_all(
                "SELECT id, crop_type FROM crops WHERE is_vf_crop = 1 ORDER BY id")
        if input_name == "crops.sale unit id":
            return fetch_all(
                "SELECT id, description FROM sale_units ORDER BY id")
        if input_name == "crops.grow system type":
            return fetch_all("SELECT id, description FROM grow_system_yield")
        raise ValueError("Unexpected input: " + str(input_name))

    @staticmethod
    def output_type():
        return {
            "crops": {
                "type": "list",
                "description": "Grow system type by crop",
                "element type": {
                    "id": {
                        "type": "integer",
                        "description": "Crop index",
                        "primary key": True
                    },
                    "system fraction": {
                        "description": "Fraction of bedspace allocated to crop",
                        "digits": 4,
                        "type": "decimal"
                    },
                    "sale unit id": {
                        "description": "Selected sale unit",
                        "digits": 0,
                        "type": "decimal"
                    },
                    "price per unit": {
                        "description": "Sale price for crop per selected unit",
                        "digits": 2,
                        "type": "decimal"
                    },
                    "grow system type": {
                        "description": "Selection for grow system type",
                        "digits": 0,
                        "type": "decimal"
                    },
                    "grow system description": {
                        "description": "Name of selected grow system",
                        "type": "text"
                    },
                    "cost per area": {
                        "description": "Equipment price per area ($ / sqft)",
                        "digits": 2,
                        "type": "decimal"
                    },
                    "cost": {
                        "description": "Total cost for equipment ($)",
                        "digits": 2,
                        "type": "decimal"
                    },
                    "depreciation": {
                        "description": "Depreciation cost per year ($ / y)",
                        "digits": 2,
                        "type": "decimal"
                    },
                    "grow system yield factor": {
                        "description": "Yield factor associated with grow system",
                        "digits": 2,
                        "type": "decimal"
                    }
                }
            },
            "cost per area": {
                "description": "Equipment price per area ($ / sqft)",
                "digits": 2,
                "type": "decimal"
            },
            "cost": {
                "description": "Total cost for equipment ($)",
                "digits": 2,
                "type": "decimal"
            },
            "depreciation": {
                "description": "Depreciation cost per year ($ / y)",
                "digits": 2,
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
            },
            "depreciation duration": {
                "description": "Duration of item depreciation",
                "default": 15,
                "type": "decimal"
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def compute(args, quantize_output=False):
        start = timeit.default_timer()
        result = dict()
        inputs = parse_inputs(Vf_Grow.input_type(), args)

        output_type = Vf_Grow.output_type()
        result['sql_runtime'] = 0
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        crop_result = []
        for row in inputs['crops']:
            grow_system_type = row['grow system type']
            grow_system_sql = "SELECT description, yield_factor FROM grow_system_yield WHERE id=%(element_id)s"
            s_start = timeit.default_timer()
            grow_system_row = fetch_one(
                grow_system_sql, {
                    'element_id': grow_system_type})
            fraction = row['system fraction']
            description = grow_system_row['description']
            cost_sql = "SELECT cost FROM grow_system_cost where (min_bedspace <= %(bedspace)s or min_bedspace is null) and description=%(description)s order by min_bedspace desc limit 1"
            cost_row = fetch_one(
                cost_sql, {
                    'bedspace': int(
                        inputs['bedspace']), 'description': description})
            result['sql_runtime'] += timeit.default_timer() - s_start
            cost_per_area = cost_row['cost']
            cost = cost_per_area * inputs['bedspace'] * fraction
            depreciation = cost / inputs["depreciation duration"]
            crop_result.append({
                'id': row['id'],
                'system fraction': fraction,
                'sale unit id': row['sale unit id'],
                'price per unit': row['price per unit'],
                'grow system type': grow_system_type,
                'grow system description': description,
                'cost per area': cost_per_area,
                'cost': cost,
                'depreciation': depreciation,
                'grow system yield factor': grow_system_row['yield_factor']
            })
        result['crops'] = crop_result

        set_if_unset(result, "cost", sum(line["cost"] for line in crop_result))
        set_if_unset(
            result,
            "cost per area",
            result["cost"] /
            inputs["bedspace"])
        set_if_unset(
            result, "depreciation", sum(
                line["depreciation"] for line in crop_result))
        result['runtime'] = timeit.default_timer() - start
        if quantize_output:
            quantize_outputs(output_type, result)

        return result