from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset, arr2dic
import math
import timeit

from models.vf.opex.labor import Vf_Labor
from models.vf.opex.rent import Vf_Rent
from models.vf.opex.cogp import Vf_Cogp
from models.vf.opex.power import Vf_Power
from models.vf.opex.water import Vf_Water


class Vf_Opex_Overview:

    @staticmethod
    def info():
        return {
            "name": "vf_opex_overview",
            "description": "Overview of Opex costs for VF"
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
                "description": "Annual number of plant sites (1 / y)",
                "type": "decimal"
            },
            "total area per week": {
                "description": "Total cultivated area per week (sqft / week)",
                "type": "decimal"
            },
            "total substrate req": {
                "description": "Total substrate requirement per year (sqft / y)",
                "type": "decimal"
            },
            "sale units": {
                "description": "Amount of sale units per year (1 / y)",
                "type": "decimal"
            },
            "seed cost per site": {
                "description": "Cost per seed ($ / plant site)",
                "default": 0.005,
                "type": "decimal"
            },
            "microgreens seed cost per site": {
                "description": "Cost per SF ($ / SF)",
                "default": 0.19,
                "type": "decimal"
            },
            "lighting power": {
                "description": "Required lighting power (kWh / y)",
                "type": "decimal"
            },
            "hvac power": {
                "description": "Required HVAC power (kWh / y)",
                "type": "decimal"
            },
            "water transpired": {
                "description": "Amount of water transpired yearly (gallons / y)",
                "type": "decimal"
            },
            "electricity cost": {
                "description": "Cost of electricity ($ / kWh)",
                "default": 0.19,
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
            "automation level": {
                "description": "Desired level of automation",
                "type": "text"
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
            },
            "lighting hours per year": {
                "description": "Required hours of lighting per year (h / y)",
                "type": "decimal"
            },
            "advanced":{
                "type": "list",
                "default": [],
                "description": "Extra info used for advanced reporting in FE",
                "element type": {
                    "description": {
                        "type": "text",
                        "description": "Line item description"
                    },
                    "value": {
                        "description": "advanced info value",
                        "type": "any"
                    }
                }
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def input_values(input_name):
        if input_name == "packaging type":
            return Vf_Cogp.input_values("packaging type")
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
                    },
                    "advanced":{
                        "type": "list",
                        "description": "Extra info used for advanced reporting in FE",
                        "element type": {
                            "description": {
                                "type": "text",
                                "description": "Line item description"
                            },
                            "value": {
                                "description": "advanced info value",
                                "type": "any"
                            }
                        }
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
        inputs = parse_inputs(Vf_Opex_Overview.input_type(), args)
        advanced_inputs = arr2dic(inputs["advanced"])

        output_type = Vf_Opex_Overview.output_type()
        result['sql_runtime'] = 0
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        args_vf_power = dict()
        args_vf_power["bedspace"] = inputs["bedspace"]
        args_vf_power["electricity cost"] = inputs["electricity cost"]
        args_vf_power["lighting power"] = inputs["lighting power"]
        args_vf_power["hvac power"] = inputs["hvac power"]
        args_vf_power["lighting hours per year"] = inputs["lighting hours per year"]
        result["vf_power"] = Vf_Power.compute(args_vf_power, quantize_output=True)

        args_vf_rent = dict()
        args_vf_rent["rent area"] = inputs["rent area"]
        args_vf_rent["rent cost"] = inputs["rent cost"]
        result["vf_rent"] = Vf_Rent.compute(args_vf_rent, quantize_output=True)

        args_vf_labor = dict()
        args_vf_labor["bedspace"] = inputs["bedspace"]
        args_vf_labor["automation level"] = inputs["automation level"]
        args_vf_labor["owner is headgrower"] = inputs["owner is headgrower"]
        args_vf_labor["headgrower salary"] = inputs["headgrower salary"]
        args_vf_labor["labor wages"] = inputs["labor wages"]
        args_vf_labor["advanced"] = inputs["advanced"]
        result["vf_labor"] = Vf_Labor.compute(args_vf_labor, quantize_output=True)
        result['sql_runtime'] += result["vf_labor"]['sql_runtime']

        set_if_unset(result, "total staff", result["vf_labor"]["total staff"])
        args_vf_cogp = dict()
        args_vf_cogp["plant sites"] = inputs["annual number of plant sites"]
        args_vf_cogp["SF substrate micros"] = inputs["total substrate req"]
        args_vf_cogp["sale units"] = inputs["sale units"]
        args_vf_cogp["packaging type"] = inputs["packaging type"]
        args_vf_cogp["seed cost per site"] = inputs["seed cost per site"]
        args_vf_cogp["microgreens seed cost per SF"] = inputs["microgreens seed cost per site"]
        result["vf_cogp"] = Vf_Cogp.compute(args_vf_cogp, quantize_output=True)
        result['sql_runtime'] += result["vf_cogp"]['sql_runtime']

        args_vf_water = dict()
        args_vf_water["water transpired"] = inputs["water transpired"]
        args_vf_water["employees needed"] = result["vf_labor"]["total staff"]
        args_vf_water["total area per week"] = inputs["total area per week"]
        result["vf_water"] = Vf_Water.compute(args_vf_water, quantize_output=True)

        if 'distribution_rate' in advanced_inputs:
            distribution_rate = advanced_inputs['distribution_rate'] / 100
        else: distribution_rate = 0.015
        
        result['line items'] = [{
            'description': 'Electricity',
            'cost': result['vf_power']['total cost'],
            'cogs cost fraction': 0.8,
            'opex cost fraction': 0.2
        }, {
            'description': 'Water',
            'cost': result['vf_water']['total water'] * inputs['water cost'],
            'cogs cost fraction': 1,
            'opex cost fraction': 0
        }, {
            'description': 'Rent',
            'cost': result['vf_rent']['total rent'],
            'cogs cost fraction': 0,
            'opex cost fraction': 1
        }, {
            'description': 'Labor',
            'cost': result['vf_labor']['total cost'],
            'cogs cost fraction': 0.8,
            'opex cost fraction': 0.2,
            'advanced': [
                {
                    "description": role['advanced name'],
                    "value": role['number of staff']
                } for role in result['vf_labor']['labor']
            ]
        }, {
            'description': 'Consumables',
            'cost': result['vf_cogp']['total cost'],
            'cogs cost fraction': 1,
            'opex cost fraction': 0
        }, {
            'description': 'Distribution',
            'cost': distribution_rate * inputs['revenue'] + 500,
            'cogs cost fraction': 0,
            'opex cost fraction': 1,
            'advanced': [
                {
                    "description": 'distribution_rate',
                    "value": distribution_rate * 100
                }
            ]
        }, {
            'description': 'Depreciation',
            'cost': inputs['depreciation'],
            'cogs cost fraction': 0,
            'opex cost fraction': 0
        }, {
            'description': 'Other',
            'cost': max(0.025 * inputs['revenue'], 1000) + 1000,
            'cogs cost fraction': 0,
            'opex cost fraction': 1
        }]
        for line in result['line items']:
            line['cogs cost'] = line['cost'] * line['cogs cost fraction']
            line['opex cost'] = line['cost'] * line['opex cost fraction']

        set_if_unset(
            result, "total cost", sum(
                line["cost"] for line in result["line items"]))

        result['runtime'] = timeit.default_timer() - start
        if quantize_output:
            quantize_outputs(output_type, result)

        return result
