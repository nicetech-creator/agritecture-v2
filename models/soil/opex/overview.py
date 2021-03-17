from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math

from models.soil.opex.labor import Soil_Labor
from models.soil.opex.rent import Soil_Rent
from models.soil.opex.cogp import Soil_Cogp
from models.soil.opex.water import Soil_Water
from models.soil.opex.energy import Soil_Energy


class Soil_Opex_Overview:

    @staticmethod
    def info():
        return {
            "name": "soil_opex_overview",
            "description": "Overview of Opex costs for Soil"
        }

    @staticmethod
    def input_type():
        return {
            "bedspace": {
                "description": "Total crop bedspace (sqft)",
                "type": "decimal"
            },
            "rent area": {
                "description": "Total area to be rented (sqft)",
                "type": "decimal"
            },
            "annual number of plant sites": {
                "description": "Annual number of plant sites",
                "type": "decimal"
            },
            "seed cost per site": {
                "description": "Cost per seed ($ / plant site)",
                "default": 0.005,
                "type": "decimal"
            },
            "water used": {
                "description": "Amount of water used yearly (gallons / y)",
                "type": "decimal"
            },
            "cold storage capex": {
                "description": "Capex for Cold Storage ($)",
                "type": "decimal"
            },
            "tools capex": {
                "description": "Capex for Tools ($)",
                "type": "decimal"
            },
            "transportation capex": {
                "description": "Capex for Transportation ($)",
                "type": "decimal"
            },
            "harvest capex": {
                "description": "Capex for Harvesting, washing and packaging ($)",
                "type": "decimal"
            },
            "water cost": {
                "description": "Cost of water ($ / gallon)",
                "default": 0.002,
                "type": "decimal"
            },
            "labor wages": {
                "description": "Projected labor wages ($ / h)",
                "default": 15.73,
                "type": "decimal"
            },
            "rent cost": {
                "description": "Rental cost per area ($ / sqft)",
                "default": 12,
                "type": "decimal"
            },
            "owner is headgrower": {
                "description": "Whether owner counts as headgrower",
                "type": "bool",
                "default": True
            },
            "headgrower salary": {
                "description": "Salary for headgrower ($ / y)",
                "default": 60000,
                "type": "decimal"
            },
            "packaging type": {
                "type": "choice",
                "description": "Selection for packaging",
                "default": 2
            },
            "revenue": {
                "description": "Initial value of wastage-corrected revenue ($ / year)",
                "type": "decimal"
            },
            "depreciation": {
                "description": "Total amount depreciated per year ($ / year)",
                "type": "decimal"
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def input_values(input_name):
        if input_name == "packaging type":
            return Soil_Cogp.input_values("packaging type")
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
                    "cogs cost": {
                        "description": "Cost assign to COGS ($)",
                        "digits": 2,
                        "type": "decimal"
                    },
                    "opex cost": {
                        "description": "Cost assigned to opex other than COGS ($)",
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
            "total cogs cost": {
                "description": "Total operating expenses cost assign to COGS ($)",
                "digits": 2,
                "type": "decimal"
            },
            "total opex cost": {
                "description": "Total operating expenses cost other than COGS ($)",
                "digits": 2,
                "type": "decimal"
            },
            "total staff": {
                "description": "Total staff on labor requirements",
                "digits": 2,
                "type": "decimal"
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def compute(args, quantize_output=False):
        result = dict()
        inputs = parse_inputs(Soil_Opex_Overview.input_type(), args)

        output_type = Soil_Opex_Overview.output_type()
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        args_soil_water = dict()
        args_soil_water["water used"] = inputs["water used"]
        args_soil_water["water cost"] = inputs["water cost"]
        result["soil_water"] = Soil_Water.compute(args_soil_water)

        args_soil_rent = dict()
        args_soil_rent["rent area"] = inputs["rent area"]
        args_soil_rent["rent cost"] = inputs["rent cost"]
        result["soil_rent"] = Soil_Rent.compute(args_soil_rent)

        args_soil_labor = dict()
        args_soil_labor["bedspace"] = inputs["bedspace"]
        args_soil_labor["owner is headgrower"] = inputs["owner is headgrower"]
        args_soil_labor["headgrower salary"] = inputs["headgrower salary"]
        args_soil_labor["labor wages"] = inputs["labor wages"]
        result["soil_labor"] = Soil_Labor.compute(args_soil_labor)

        set_if_unset(
            result,
            "total staff",
            result["soil_labor"]["total staff"])
        args_soil_cogp = dict()
        args_soil_cogp["plant sites"] = inputs["annual number of plant sites"]
        args_soil_cogp["packaging type"] = inputs["packaging type"]
        args_soil_cogp["seed cost per site"] = inputs["seed cost per site"]
        result["soil_cogp"] = Soil_Cogp.compute(args_soil_cogp)

        args_soil_energy = dict()
        args_soil_energy["cold storage capex"] = inputs["cold storage capex"]
        args_soil_energy["tools capex"] = inputs["tools capex"]
        args_soil_energy["transportation capex"] = inputs["transportation capex"]
        args_soil_energy["harvest capex"] = inputs["harvest capex"]
        result["soil_energy"] = Soil_Energy.compute(args_soil_energy)

        result['line items'] = [{
            'description': 'Water',
            'cost': result['soil_water']['total cost'],
            'cogs cost fraction': 1,
            'opex cost fraction': 0
        }, {
            'description': 'Rent',
            'cost': result['soil_rent']['total rent'],
            'cogs cost fraction': 0,
            'opex cost fraction': 1
        }, {
            'description': 'Labor',
            'cost': result['soil_labor']['total cost'],
            'cogs cost fraction': 0.8,
            'opex cost fraction': 0.2
        }, {
            'description': 'Energy (fuel + electricity)',
            'cost': result['soil_energy']['total cost'],
            'cogs cost fraction': 0.8,
            'opex cost fraction': 0.2
        }, {
            'description': 'Consumables',
            'cost': result['soil_cogp']['total cost'],
            'cogs cost fraction': 1,
            'opex cost fraction': 0
        }, {
            'description': 'Distribution',
            'cost': 0.015 * inputs['revenue'],
            'cogs cost fraction': 0,
            'opex cost fraction': 1
        }, {
            'description': 'Depreciation',
            'cost': inputs['depreciation'],
            'cogs cost fraction': 0,
            'opex cost fraction': 0
        }, {
            'description': 'Other',
            'cost': 0,
            'cogs cost fraction': 0,
            'opex cost fraction': 1
        }, {
            'description': 'Admin',
            'cost': 0.05 * result['soil_labor']['total cost'],
            'cogs cost fraction': 0,
            'opex cost fraction': 1
        }]
        for line in result['line items']:
            line['cogs cost'] = line['cost'] * line['cogs cost fraction']
            line['opex cost'] = line['cost'] * line['opex cost fraction']

        set_if_unset(
            result, "total cost", sum(
                line["cost"] for line in result["line items"]))

        if quantize_output:
            quantize_outputs(output_type, result)

        return result