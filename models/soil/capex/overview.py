from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math


class Soil_Capex_Overview:

    @staticmethod
    def info():
        return {
            "name": "soil_capex_overview",
            "description": "Overview of Capex costs for Soil"
        }

    @staticmethod
    def input_type():
        return {
            "land area": {
                "description": "Total land area (sqft)",
                "type": "decimal"
            },
            "nursery trays": {
                "description": "Number of trays needed in nursery",
                "type": "decimal"
            },
            "real estate cost": {
                "description": "Cost of real estate ($)",
                "type": "decimal"
            },
            "seeding equipment cost": {
                "description": "Cost of seeding equipment ($)",
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
            "line items": {
                "type": "list",
                "description": "Line item costs",
                "element type": {
                    "description": {
                        "type": "text",
                        "description": "Line item description"
                    },
                    "cost": {
                        "description": "Line item expenses cost ($)",
                        "digits": 2,
                        "type": "decimal"
                    },
                    "depreciation": {
                        "description": "Depreciation amount per year ($ / y)",
                        "digits": 2,
                        "type": "decimal"
                    }
                }
            },
            "total cost": {
                "description": "Total operating expenses cost ($)",
                "digits": 2,
                "type": "decimal"
            },
            "total depreciation": {
                "description": "Total depreciation per year ($ / y)",
                "digits": 2,
                "type": "decimal"
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def compute(args, quantize_output=False):
        result = dict()
        inputs = parse_inputs(Soil_Capex_Overview.input_type(), args)

        output_type = Soil_Capex_Overview.output_type()
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        query_inputs = {"land_area": inputs["land area"]}
        price_row = fetch_one(
            "select price_irrigation, price_fertility, price_groundwork, price_nursery from soil_capex_lookup where max_bedspace is null or max_bedspace > %(land_area)s order by id limit 1",
            query_inputs)
        if price_row is None:
            raise ValueError(
                "No elements found for inputs: " +
                str(query_inputs))

        land_area = inputs['land area']
        result['line items'] = [{
            'description': 'Cold Storage',
            'cost': land_area * 0.09,
            'depreciation': land_area * 0.09 / 8
        }, {
            'description': 'Tools',
            'cost': land_area * 0.09,
            'depreciation': land_area * 0.09 / 8
        }, {
            'description': 'Transportation',
            'cost': land_area * 0.12,
            'depreciation': 0
        }, {
            'description': 'Irrigation - drip',
            'cost': price_row['price_irrigation'] * land_area,
            'depreciation': price_row['price_irrigation'] * land_area / 8
        }, {
            'description': 'Initial Fertility Upgrade',
            'cost': price_row['price_fertility'] * land_area,
            'depreciation': 0
        }, {
            'description': 'Initial Ground Work - bed shaping',
            'cost': price_row['price_groundwork'] * land_area,
            'depreciation': 0
        }, {
            'description': 'Other Capex - fixed price',
            'cost': 0,
            'depreciation': 0
        }, {
            'description': 'Indoor / Covered Nursery with t-5s',
            'cost':  price_row['price_nursery'] * inputs['nursery trays'],
            'depreciation': 0
        }, {
            'description': 'Harvesting, washing & packaging equipment',
            'cost':  land_area * 0.085,
            'depreciation': land_area * 0.085 / 8
        }, {
            'description': 'Seeding Equipment',
            'cost': inputs['seeding equipment cost'],
            'depreciation': inputs['seeding equipment cost'] / 8
        }, {
            'description': 'Real Estate',
            'cost': inputs['real estate cost'],
            'depreciation': 0
        }]

        set_if_unset(
            result, "total cost", sum(
                line["cost"] for line in result["line items"]))
        set_if_unset(result, "total depreciation", sum(
            line["depreciation"] for line in result["line items"]))

        if quantize_output:
            quantize_outputs(output_type, result)

        return result
