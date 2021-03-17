from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math
import timeit

class Vf_Crop:

    @staticmethod
    def info():
        return {
            "name": "vf_crop",
            "description": "Crop matrix selector for VF"
        }

    @staticmethod
    def input_type():
        return {
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
                    },
                    "grow system yield factor": {
                        "description": "Yield factor from grow system",
                        "default": 1.03,
                        "type": "decimal"
                    }
                }
            },
            "provided light sum": {
                "description": "Yearly sum of DLI (mol / m2 d)",
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
                    "grow cycle": {
                        "digits": 0,
                        "description": "Duration of a grow cycle (d)",
                        "type": "decimal"
                    },
                    "cycles": {
                        "digits": 0,
                        "description": "Number of cycles per year (1 / y)",
                        "type": "decimal"
                    },
                    "area per week": {
                        "digits": 0,
                        "description": "Area cultivated (sqft / week)",
                        "type": "decimal"
                    },
                    "plant sites per area": {
                        "digits": 1,
                        "description": "Number of plant sites per sqft (1 / sqft)",
                        "type": "decimal"
                    },
                    "plant sites": {
                        "digits": 0,
                        "description": "Number of plant sites per year (1 / y)",
                        "type": "decimal"
                    },
                    "substrate req": {
                        "digits": 2,
                        "description": "Substrate requirement per year (sqft / y)",
                        "type": "decimal"
                    },
                    "yield factor": {
                        "digits": 2,
                        "description": "Yield factor due to yearly light sum",
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
                    "bunch / head": {
                        "digits": 0,
                        "description": "Bunch / heads per year (1 / y)",
                        "type": "decimal"
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
                    },
                    "system fraction": {
                        "description": "Fraction of bedspace allocated to crop",
                        "type": "decimal",
                        "digits": 2
                    },
                    "sale unit id": {
                        "type": "choice",
                        "description": "Selected sale unit for crop item"
                    },
                }
            },
            "total plant sites": {
                "digits": 0,
                "description": "Total number of plant sites per year (1 / y)",
                "type": "decimal"
            },
            "total substrate req": {
                "digits": 0,
                "description": "Total substrate requirement per year (sqft / y)",
                "type": "decimal"
            },
            "total area per week": {
                "digits": 0,
                "description": "Total cultivated area per week (sqft / week)",
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
        inputs = parse_inputs(Vf_Crop.input_type(), args)

        output_type = Vf_Crop.output_type()
        result['sql_runtime'] = 0
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        s_start = timeit.default_timer()
        sale_unit_lookup = {r['id']: (r['description'], r['multiplier'], r['lb']) for r in fetch_all(
            "SELECT id, description, multiplier, lb FROM sale_units")}
        result['sql_runtime'] += timeit.default_timer() - s_start

        def get_lb(line):
            return sale_unit_lookup[line['sale unit id']][2]

        def get_multiplier(line):
            return sale_unit_lookup[line['sale unit id']][1]

        def get_description(line):
            return sale_unit_lookup[line['sale unit id']][0]

        bedspace = inputs['bedspace']
        provided_light_sum = inputs['provided light sum']
        sql = "SELECT * FROM crops WHERE is_vf_crop = 1 and id=%(id)s"
        result_crops = []
        for input_row in inputs['crops']:
            s_start = timeit.default_timer()
            row = fetch_one(sql, {'id': input_row['id']})
            result['sql_runtime'] += timeit.default_timer() - s_start
            assert row is not None, 'Could not find crop with ID: ' + \
                str(input_row['id'])
            line = dict()
            line['id'] = row['id']
            line['crop type'] = row['crop_type']
            lb = get_lb(input_row)
            multiplier = get_multiplier(input_row)
            description = get_description(input_row)
            line['area'] = input_row['system fraction'] * bedspace
            line['grow cycle'] = row['optimal_crop_cycle']
            line['cycles'] = 365 / row['optimal_crop_cycle']
            line['area per week'] = line['area'] * line['cycles'] / 52
            line['plant sites per area'] = row['plants_per_sqft']
            line['plant sites'] = line['plant sites per area'] * \
                line['area'] * line['cycles']
            line['substrate req'] = row['substrate_requirement'] * \
                line['area'] * line['cycles']
            line['yield factor'] = min(
                provided_light_sum / (row['light_sum__mol_m2'] * (365 / row['optimal_crop_cycle'])), 1)
            line['yield'] = line['yield factor'] * input_row['grow system yield factor'] * \
                row['lbs_per_plant_per_cycle'] * \
                (line['plant sites'] + line['substrate req'])
            line['sale unit type'] = description
            line['sale unit id'] = input_row['sale unit id']
            line['sale units'] = line['plant sites'] if lb is None else line['yield'] * lb
            line['bunch / head'] = line['plant sites'] if multiplier is None else 0
            line['water use'] = line['yield'] * 20 / 8.34
            line['price per unit'] = input_row['price per unit']
            line['revenue'] = line['sale units'] * input_row['price per unit']
            line['lost factors'] = []
            for y in range(1, 16):
                line['lost factors'].append({"lost": row['yr' + str(y)]})
            line['system fraction'] = input_row['system fraction']
            result_crops.append(line)
        result['crops'] = result_crops

        set_if_unset(result, "total plant sites", sum(
            line["plant sites"] for line in result["crops"]))
        set_if_unset(result, "total substrate req", sum(
            line["substrate req"] for line in result["crops"]))
        set_if_unset(result, "total area per week", sum(
            line["area per week"] for line in result["crops"]))
        set_if_unset(
            result, "total yield", sum(
                line["yield"] for line in result["crops"]))
        set_if_unset(result, "total water use", sum(
            line["water use"] for line in result["crops"]))
        set_if_unset(result, "total sale units", sum(
            line["sale units"] for line in result["crops"]))
        set_if_unset(result, "total revenue", sum(
            line["revenue"] for line in result["crops"]))

        result['runtime'] = timeit.default_timer() - start
        if quantize_output:
            quantize_outputs(output_type, result)

        return result