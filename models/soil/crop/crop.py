from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math

from models.soil.crop.cycles import Soil_Crop_Cycles


class Soil_Crop:

    @staticmethod
    def info():
        return {
            "name": "soil_crop",
            "description": "Crop matrix selector for Soil"
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
            "bedspace": {
                "description": "Bedspace area to be allocated to crops (sqft)",
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
                    }
                }
            },
            "complementary crop id": {
                "type": "choice",
                "description": "Selected crop as complementary crop",
                "default": 1
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def input_values(input_name):
        if input_name == "crops.id":
            return fetch_all("SELECT id, crop_type FROM crops ORDER BY id")
        if input_name == "crops.sale unit id":
            return fetch_all(
                "SELECT id, description FROM sale_units ORDER BY id")
        if input_name == "complementary crop id":
            return fetch_all("SELECT id, crop_type FROM crops ORDER BY id")
        raise ValueError("Unexpected input: " + str(input_name))

    @staticmethod
    def output_type():
        return {
            "crops": {
                "type": "list",
                "description": "Crop matrix results",
                "element type": {
                    "id": {
                        "type": "integer",
                        "description": "Crop index",
                        "primary key": True
                    },
                    "crop type": {
                        "type": "text",
                        "description": "Name of the crop"
                    },
                    "area": {
                        "digits": 0,
                        "description": "Area covered by crop (sqft)",
                        "type": "decimal"
                    },
                    "cycles": {
                        "digits": 0,
                        "description": "Number of cycles per year (1 / y)",
                        "type": "decimal"
                    },
                    "plant sites": {
                        "digits": 0,
                        "description": "Number of plant sites per year (1 / y)",
                        "type": "decimal"
                    },
                    "yield": {
                        "digits": 0,
                        "description": "Yield (lb / y)",
                        "type": "decimal"
                    },
                    "lost factors": {
                        "description": "Factors presenting lost due to first year harvest",
                        "type": "list",
                        "element type": {
                            "lost": {
                                "type": "decimal",
                                "digits": 2
                            }
                        }
                    }, 
                    "water use": {
                        "digits": 0,
                        "description": "Water use per year (gallons / y)",
                        "type": "decimal"
                    },
                    "sale unit type": {
                        "type": "text",
                        "description": "Type of sale unit"
                    },
                    "sale units": {
                        "digits": 0,
                        "description": "Number of sale units per year (1 / y)",
                        "type": "decimal"
                    },
                    "price per unit": {
                        "digits": 2,
                        "description": "Price obtained per unit ($ / unit)",
                        "type": "decimal"
                    },
                    "revenue": {
                        "digits": 2,
                        "description": "Revenue collected per year ($ / y)",
                        "type": "decimal"
                    }
                }
            },
            "total plant sites": {
                "digits": 0,
                "description": "Total number of plant sites per year (1 / y)",
                "type": "decimal"
            },
            "total yield": {
                "digits": 0,
                "description": "Total yield per year (lb / y)",
                "type": "decimal"
            },
            "total water use": {
                "digits": 0,
                "description": "Total water use per year (gallons / y)",
                "type": "decimal"
            },
            "total sale units": {
                "digits": 0,
                "description": "Total sale units per year (1 / y)",
                "type": "decimal"
            },
            "total revenue": {
                "digits": 2,
                "description": "Total revenue per year ($ / y)",
                "type": "decimal"
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def compute(args, quantize_output=False):
        result = dict()
        inputs = parse_inputs(Soil_Crop.input_type(), args)

        output_type = Soil_Crop.output_type()
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        sql_crop_parameters = "SELECT temp_absolute_min, temp_optimal_min, temp_optimal_max, temp_absolute_max, optimal_crop_cycle FROM crops WHERE id=%(id)s"
        complementary_crop_row = fetch_one(
            sql_crop_parameters, {
                'id': inputs['complementary crop id']})
        crop_cycle_crops = []
        for input_row in inputs['crops']:
            line = dict()
            crop_row = fetch_one(sql_crop_parameters, {'id': input_row['id']})
            line['id'] = str(input_row['id'])
            line['absolute min temp'] = crop_row['temp_absolute_min']
            line['optimal min temp'] = crop_row['temp_optimal_min']
            line['optimal max temp'] = crop_row['temp_optimal_max']
            line['absolute max temp'] = crop_row['temp_absolute_max']
            line['optimal crop cycle'] = crop_row['optimal_crop_cycle']
            crop_cycle_crops.append(line)

        args_soil_crop_cycles = dict()
        args_soil_crop_cycles["latitude"] = inputs["latitude"]
        args_soil_crop_cycles["longitude"] = inputs["longitude"]
        args_soil_crop_cycles["crops"] = crop_cycle_crops
        args_soil_crop_cycles["complementary crop absolute min temp"] = complementary_crop_row["temp_absolute_min"]
        args_soil_crop_cycles["complementary crop optimal min temp"] = complementary_crop_row["temp_optimal_min"]
        args_soil_crop_cycles["complementary crop optimal max temp"] = complementary_crop_row["temp_optimal_max"]
        args_soil_crop_cycles["complementary crop absolute max temp"] = complementary_crop_row["temp_absolute_max"]
        args_soil_crop_cycles["complementary crop optimal crop cycle"] = complementary_crop_row["optimal_crop_cycle"]
        result["soil_crop_cycles"] = Soil_Crop_Cycles.compute(
            args_soil_crop_cycles)

        crop_cycles_result = result['soil_crop_cycles']['crops']

        def get_crop_cycle_info(crop_id):
            for line in crop_cycles_result:
                if str(line['id']) == str(crop_id):
                    return line
            return None

        sale_unit_lookup = {r['id']: (r['description'], r['multiplier']) for r in fetch_all(
            "SELECT id, description, multiplier FROM sale_units")}

        def get_multiplier(line):
            return sale_unit_lookup[line['sale unit id']][1]

        def get_description(line):
            return sale_unit_lookup[line['sale unit id']][0]

        bedspace = inputs['bedspace']
        sql = "SELECT * FROM crops WHERE id=%(id)s"
        result_crops = []
        for input_row in inputs['crops']:
            row = fetch_one(sql, {'id': input_row['id']})
            assert row is not None, 'Could not find crop with ID: ' + \
                str(input_row['id'])
            line = dict()
            line['id'] = row['id']
            line['crop type'] = row['crop_type']
            multiplier = get_multiplier(input_row)
            description = get_description(input_row)
            line['system fraction'] = input_row['system fraction']
            line['area'] = input_row['system fraction'] * bedspace
            x = get_crop_cycle_info(row['id'])
            line['cycles'] = x['main crop cycles']
            line['complementary cycles'] = x['complementary crop cycles']
            line['plant sites'] = row['plants_per_sqft'] * \
                line['area'] * line['cycles']
            substrate_req = row['substrate_requirement'] * \
                line['area'] * line['cycles']
            line['yield'] = row['lbs_per_plant_per_cycle'] * \
                (line['plant sites'] + substrate_req)
            line['water use'] = line['yield'] * 20 / 8.34
            line['sale unit id'] = input_row['sale unit id']
            line['sale unit type'] = description
            line['sale units'] = line['plant sites'] if multiplier is None else line['yield'] * multiplier
            line['price per unit'] = input_row['price per unit']
            line['revenue'] = line['sale units'] * line['price per unit']
            line['lost factors'] = []
            for y in range(1, 16):
                line['lost factors'].append({"lost": row['yr' + str(y)]})
            result_crops.append(line)

        def get_default_price_per_unit(crop_id, sale_unit_id):
            sql = '''
                select
                        case
                                when u.multiplier is null then default_price_per_each_market
                                else default_price_per_lb_market * u.multiplier
                        end price_per_unit
                from
                        crops c
                        cross join sale_units u
                where
                        c.id = %(crop_id)s and u.id = %(sale_unit_id)s
            '''
            query_inputs = {
                'crop_id': str(crop_id),
                'sale_unit_id': str(sale_unit_id)
            }
            price_row = fetch_one(sql, query_inputs)
            return price_row['price_per_unit']

        def get_default_revenue():
            revenue = 0
            for line in result_crops:
                default_price = get_default_price_per_unit(
                    line['id'], line['sale unit id'])
                revenue += default_price * line['sale units']
            return revenue

        def get_price_factor():
            total_main_revenue = sum(line['revenue'] for line in result_crops)
            total_main_revenue_default = get_default_revenue()
            return total_main_revenue / total_main_revenue_default
        # Add row for complementary crop
        row = fetch_one(sql, {'id': inputs['complementary crop id']})
        assert row is not None, 'Could not find complementary crop with ID: ' + \
            str(inputs['complementary crop id'])
        line = dict()
        line['id'] = inputs['complementary crop id']
        line['crop type'] = row['crop_type']
        multiplier = 1
        description = '1 lb'
        line['area'] = sum(r['area'] for r in result_crops)
        line['cycles'] = sum(r['system fraction'] *
                             r['complementary cycles'] for r in result_crops)
        line['plant sites'] = row['plants_per_sqft'] * \
            line['area'] * line['cycles']
        line['yield'] = row['lbs_per_plant_per_cycle'] * \
            (line['plant sites'] + substrate_req)
        line['water use'] = line['yield'] * 20 / 8.34
        line['sale unit type'] = description
        line['sale units'] = line['yield'] * multiplier
        line['price per unit'] = row['default_price_per_lb_market'] * \
            get_price_factor()
        line['revenue'] = line['sale units'] * line['price per unit']
        result_crops.append(line)
        result['crops'] = result_crops

        set_if_unset(result, "total plant sites", sum(
            line["plant sites"] for line in result["crops"]))
        set_if_unset(
            result, "total yield", sum(
                line["yield"] for line in result["crops"]))
        set_if_unset(result, "total water use", sum(
            line["water use"] for line in result["crops"]))
        set_if_unset(result, "total sale units", sum(
            line["sale units"] for line in result["crops"]))
        set_if_unset(result, "total revenue", sum(
            line["revenue"] for line in result["crops"]))

        if quantize_output:
            quantize_outputs(output_type, result)

        return result