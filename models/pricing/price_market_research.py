from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math


class Price_Market_Research:

    @staticmethod
    def info():
        return {
            "name": "price_market_research",
            "description": "Calculates estimated price from market research for one crop"
        }

    @staticmethod
    def input_type():
        return {
            "crop id": {
                "type": "choice",
                "description": "Selected crop item"
            },
            "result sale unit id": {
                "type": "choice",
                "description": "Selected sale unit for crop item",
                "default": 0
            },
            "data": {
                "type": "list",
                "description": "Market research data points",
                "default": [],
                "element type": {
                    "price per unit": {
                        "description": "Sale price for crop per selected unit ($ / unit)",
                        "type": "decimal"
                    },
                    "sale unit id": {
                        "type": "choice",
                        "description": "Selected sale unit for crop item"
                    },
                    "quality score": {
                        "type": "choice",
                        "description": "Selected quality score for item (1 - 5)"
                    }
                }
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def input_values(input_name):
        if input_name == "all":
            return {
                "crops": fetch_all(
                    "SELECT id, reduced_level_height, default_price_per_lb_market, is_gh_crop, is_vf_crop, crop_type, crop_category, nft, ebb_flow, deep_water_culture, bucket, slab, soil, aeroponic FROM crops ORDER BY id"),
                "sale_uint_options":  fetch_all(
                    "SELECT * FROM sale_units ORDER BY id"),
                "quality_score_options": [{'id': 1, 'description': '1', 'multiplier': 0.7}, {'id': 2, 'description': '2', 'multiplier': 0.8}, {
                    'id': 3, 'description': '3', 'multiplier': 0.9}, {'id': 4, 'description': '4', 'multiplier': 1.0}, {'id': 5, 'description': '5', 'multiplier': 1.1} ],
                "packaging_options": fetch_all(
                    "SELECT * FROM packaging ORDER BY id"),
                "price_record_options": fetch_all(
                    "SELECT * FROM price_record ORDER BY id"),
                "price_type_options": fetch_all(
                    "SELECT * FROM price_types ORDER BY id"),
                "price_label_options": fetch_all(
                    "SELECT * FROM product_label ORDER BY id")
            }
        if input_name == "crop id":
            return fetch_all("SELECT id, crop_type, crop_category, nft, ebb_flow, deep_water_culture, bucket, slab, soil, aeroponic FROM crops ORDER BY id")
        if input_name == "result sale unit id":
            return fetch_all(
                "SELECT 0 id, '(autoselect)' description UNION ALL SELECT id, description FROM sale_units ORDER BY id")
        if input_name == "data.sale unit id":
            return fetch_all(
                "SELECT * FROM sale_units ORDER BY id")
        if input_name == "data.quality score":
            return [{'id': 1, 'description': '1', 'multiplier': 0.7}, {'id': 2, 'description': '2', 'multiplier': 0.8}, {
                'id': 3, 'description': '3', 'multiplier': 0.9}, {'id': 4, 'description': '4', 'multiplier': 1.0}, {'id': 5, 'description': '5', 'multiplier': 1.1} ]
        if input_name == "packaging id":
            return fetch_all(
                "SELECT * FROM packaging ORDER BY id")
        if input_name == "product labels id":
            return fetch_all(
                "SELECT * FROM product_label ORDER BY id")
        if input_name == "sale channel id":
            return fetch_all(
                "SELECT * FROM sale_channel ORDER BY id")
        if input_name == "price records":
            return fetch_all(
                "SELECT * FROM price_record ORDER BY id")
        if input_name == "price types":
            return fetch_all(
                "SELECT * FROM price_types ORDER BY id")

        raise ValueError("Unexpected input: " + str(input_name))

    @staticmethod
    def output_type():
        return {
            "price": {
                "description": "Sale price for crop per selected unit ($ / unit)",
                "digits": 2,
                "type": "decimal"
            },
            "crop id": {
                "description": "Selected crop item",
                "digits": 0,
                "type": "decimal"
            },
            "result sale unit id": {
                "description": "Selected sale unit for crop item",
                "digits": 0,
                "type": "decimal"
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def compute(args, quantize_output=False):
        result = dict()
        inputs = parse_inputs(Price_Market_Research.input_type(), args)

        output_type = Price_Market_Research.output_type()
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        result['crop id'] = inputs['crop id']

        # Autoselect sale unit id if needed
        result_sale_unit_id = inputs['result sale unit id']
        if result_sale_unit_id == 0:
            sql = "select case when default_price_per_lb_market is not null then 6 else 7 end default_sale_unit_id from crops where id = %(crop_id)s"
            row = fetch_one(sql, {'crop_id': str(inputs['crop id'])})
            result_sale_unit_id = row['default_sale_unit_id']
        # Always specify unit in result
        result['result sale unit id'] = result_sale_unit_id

        quality_score_map = {1: 0.75, 2: 0.85, 3: 0.95, 4: 1.00, 5: 1.05}

        lbs_sql = "SELECT lbs_per_plant_per_cycle FROM crops WHERE id=%(id)s"
        row_weight = fetch_one(lbs_sql, {'id': inputs['crop id']})
        default_lbs_per_plant = row_weight['lbs_per_plant_per_cycle']
        if default_lbs_per_plant == 0 or default_lbs_per_plant is None:
            default_lbs_per_plant = 1

        sale_unit_sql = "SELECT id, (1.0 / multiplier) multiplier FROM sale_units"
        sale_unit_data = {row['id']: row['multiplier']
                          for row in fetch_all(sale_unit_sql)}

        from decimal import Decimal

        def get_default_price():
            # sql = '''
            #     select
            #             case
            #                     when u.multiplier is null then
            #                             coalesce(default_price_per_each_market, default_price_per_lb_market / (case when lbs_per_plant_per_cycle is null or lbs_per_plant_per_cycle = 0 then 1 else lbs_per_plant_per_cycle end))
            #                     else
            #                             (1.0 / u.multiplier) * coalesce(default_price_per_lb_market, default_price_per_each_market * (case when lbs_per_plant_per_cycle is null or lbs_per_plant_per_cycle = 0 then 1 else lbs_per_plant_per_cycle end))
            #             end price_per_unit
            #     from
            #             crops c cross join sale_units u
            #     where
            #             c.id = %(crop_id)s and u.id = %(sale_unit_id)s
            # '''

            sql = '''
                select default_price_per_lb_market as price_per_unit
                from crops
                where id = %(crop_id)s
            '''
            query_inputs = {
                'crop_id': str(inputs['crop id']),
                'sale_unit_id': str(result_sale_unit_id)
            }
            price_row = fetch_one(sql, query_inputs)
            price = price_row['price_per_unit']
            return price

        num_points = len(inputs['data'])
        if num_points == 0:
            result['price'] = get_default_price()
        else:
            weighted_sum = 0
            for input_row in inputs['data']:
                price_per_unit = input_row['price per unit'] / \
                    quality_score_map[input_row['quality score']]
                sale_unit_multiplier = sale_unit_data[input_row['sale unit id']]
                if sale_unit_multiplier is None:
                    weighted_sum += price_per_unit
                else:
                    weighted_sum += price_per_unit * \
                        (default_lbs_per_plant / sale_unit_multiplier)
            result_sale_unit_multiplier = sale_unit_data[result_sale_unit_id]
            if result_sale_unit_multiplier is None:
                result['price'] = weighted_sum / num_points
            else:
                result['price'] = (weighted_sum / num_points) * \
                    (result_sale_unit_multiplier / default_lbs_per_plant)

        if quantize_output:
            quantize_outputs(output_type, result)

        return result