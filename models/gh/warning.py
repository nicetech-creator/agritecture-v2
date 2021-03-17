from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math

from models.gh.capex.grow import Gh_Grow


class Gh_Warning:

    @staticmethod
    def info():
        return {
            "name": "gh_warning",
            "description": "Warning list for GH"
        }

    @staticmethod
    def input_type():
        return {
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
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def input_values(input_name):
        if input_name == "crops.id":
            return fetch_all(
                "SELECT id, crop_type FROM crops WHERE is_gh_crop = 1 ORDER BY id")
        if input_name == "crops.sale unit id":
            return fetch_all(
                "SELECT id, description FROM sale_units ORDER BY id")
        if input_name == "crops.grow system type":
            return Gh_Grow.input_values("crops.grow system type")
        raise ValueError("Unexpected input: " + str(input_name))

    @staticmethod
    def output_type():
        return {
            "warnings": {
                "type": "list",
                "description": "List of warnings",
                "element type": {
                    "severity": {
                        "type": "text",
                        "description": "Severity level of warning"
                    },
                    "message": {
                        "type": "text",
                        "description": "Warning message"
                    }
                }
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def compute(args, quantize_output=False):
        result = dict()
        inputs = parse_inputs(Gh_Warning.input_type(), args)

        output_type = Gh_Warning.output_type()
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        result['warnings'] = []
        map_grow_system_to_column = {
            'NFT': 'nft',
            'Ebb/Flow': 'ebb_flow',
            'Deep Water Culture': 'deep_water_culture',
            'Bucket': 'bucket',
            'Slab': 'slab',
            'Soil': 'soil',
            'Aeroponic': 'aeroponic'
        }
        for row in inputs['crops']:
            crop_id = row['id']
            sale_unit_id = row['sale unit id']
            crop_data_row = fetch_one(
                "SELECT * FROM crops WHERE id=%(id)s", {"id": crop_id})
            sale_type_row = fetch_one(
                "SELECT description, multiplier FROM sale_units WHERE id=%(id)s", {
                    "id": sale_unit_id})
            cannot_sell_per_lb = crop_data_row['default_price_per_lb_market'] is None
            cannot_sell_per_each = crop_data_row['default_price_per_each_market'] is None
            sale_type_per_each = sale_type_row['multiplier'] is None
            # Warnings for invalid crops for GH
            if crop_data_row['is_gh_crop'] != 1:
                result['warnings'].append({
                    'severity': 'error',
                    'message': 'Based on current available data, we cannot accurately model "' + crop_data_row['crop_type'] + '" to be grown in greenhouses"'
                })
            # Warnings for invalid crop sale units
            if sale_type_per_each and cannot_sell_per_each:
                result['warnings'].append({
                    'severity': 'error',
                    'message': 'Cannot sell "' + crop_data_row['crop_type'] + '" in sale unit "' + sale_type_row['description'] + '"'
                })
            elif not sale_type_per_each and cannot_sell_per_lb:
                result['warnings'].append({
                    'severity': 'error',
                    'message': 'Cannot sell "' + crop_data_row['crop_type'] + '" in sale unit "' + sale_type_row['description'] + '"'
                })
            # Warnings for invalid crop + grow system combination
            grow_system_sql = "SELECT description FROM grow_system_yield WHERE id=%(element_id)s"
            grow_system_row = fetch_one(
                grow_system_sql, {
                    'element_id': row['grow system type']})
            grow_system_description = grow_system_row['description']
            grow_system_column = map_grow_system_to_column[grow_system_description]
            if crop_data_row[grow_system_column] == 0:
                result['warnings'].append({
                    'severity': 'error',
                    'message': 'Based on current available data, we cannot accurately model "' + crop_data_row['crop_type'] + '" to be grown in "' + grow_system_description + '"'
                })
        # Warnings for total system fraction
        from decimal import Decimal
        percentage_value = Decimal(
            0) + sum(Decimal(str(row['system fraction'])) for row in inputs['crops']).quantize(Decimal(10) ** -4) * 100
        if percentage_value < 100:
            result['warnings'].append({
                'severity': 'warning',
                'message': 'Only allocated ' + str(percentage_value.quantize(1)) + '% of available bedspace, under limit of 100%'
            })
        elif percentage_value > 100:
            result['warnings'].append({
                'severity': 'error',
                'message': 'Allocated ' + str(percentage_value.quantize(1)) + '% of available bedspace, over limit of 100%'
            })

        if quantize_output:
            quantize_outputs(output_type, result)

        return result
