from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset, arr2dic
import math
import timeit

from models.gh.opex.heating import Gh_Heating_Opex
from models.gh.opex.power import Gh_Power
from models.gh.opex.labor import Gh_Labor
from models.gh.opex.rent import Gh_Rent
from models.gh.opex.cogp import Gh_Cogp
from models.gh.opex.water import Gh_Water


class Gh_Opex_Overview:

    @staticmethod
    def info():
        return {
            "name": "gh_opex_overview",
            "description": "Overview of Opex costs for GH"
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
            "heating required": {
                "description": "Total energy used for heating (kWh / y)",
                "type": "decimal"
            },
            "ventilation power": {
                "description": "Required ventilation power (kWh / y)",
                "type": "decimal"
            },
            "lighting power density": {
                "description": "Required lighting power density (kW / sqft)",
                "type": "decimal"
            },
            "irrigation water": {
                "description": "Amount of water used by irrigation (L / y)",
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
            "gas cost": {
                "description": "Cost of gas",
                "default": 8.5,
                "type": "decimal"
            },
            "gas cost unit id": {
                "description": "Selected unit for gas cost",
                "default": 2,
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
        if input_name == "gas cost unit id":
            return fetch_all(
                "SELECT id, description FROM gas_cost_units ORDER BY id")
        if input_name == "packaging type":
            return Gh_Cogp.input_values("packaging type")
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
        inputs = parse_inputs(Gh_Opex_Overview.input_type(), args)
        advanced_inputs = arr2dic(inputs["advanced"])
        output_type = Gh_Opex_Overview.output_type()
        result['sql_runtime'] = 0
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        args_gh_heating_opex = dict()
        args_gh_heating_opex["heating required"] = inputs["heating required"]
        result["gh_heating_opex"] = Gh_Heating_Opex.compute(
            args_gh_heating_opex, quantize_output=True)

        args_gh_power = dict()
        args_gh_power["bedspace"] = inputs["bedspace"]
        args_gh_power["electricity cost"] = inputs["electricity cost"]
        args_gh_power["ventilation power"] = inputs["ventilation power"]
        args_gh_power["lighting power density"] = inputs["lighting power density"]
        args_gh_power["lighting hours per year"] = inputs["lighting hours per year"]
        result["gh_power"] = Gh_Power.compute(args_gh_power, quantize_output=True)

        args_gh_rent = dict()
        args_gh_rent["rent area"] = inputs["rent area"]
        args_gh_rent["rent cost"] = inputs["rent cost"]
        result["gh_rent"] = Gh_Rent.compute(args_gh_rent, quantize_output=True)

        args_gh_labor = dict()
        args_gh_labor["bedspace"] = inputs["bedspace"]
        args_gh_labor["automation level"] = inputs["automation level"]
        args_gh_labor["owner is headgrower"] = inputs["owner is headgrower"]
        args_gh_labor["headgrower salary"] = inputs["headgrower salary"]
        args_gh_labor["labor wages"] = inputs["labor wages"]
        args_gh_labor["advanced"] = inputs["advanced"] 
        result["gh_labor"] = Gh_Labor.compute(args_gh_labor, quantize_output=True)
        result['sql_runtime'] += result["gh_labor"]['sql_runtime']

        set_if_unset(result, "total staff", result["gh_labor"]["total staff"])
        args_gh_cogp = dict()
        args_gh_cogp["plant sites"] = inputs["annual number of plant sites"]
        args_gh_cogp["SF substrate micros"] = inputs["total substrate req"]
        args_gh_cogp["sale units"] = inputs["sale units"]
        args_gh_cogp["packaging type"] = inputs["packaging type"]
        args_gh_cogp["seed cost per site"] = inputs["seed cost per site"]
        args_gh_cogp["microgreens seed cost per SF"] = inputs["microgreens seed cost per site"]
        result["gh_cogp"] = Gh_Cogp.compute(args_gh_cogp, quantize_output=True)
        result['sql_runtime'] += result["gh_cogp"]['sql_runtime']

        args_gh_water = dict()
        args_gh_water["water transpired"] = inputs["irrigation water"] * 0.264172
        args_gh_water["employees needed"] = result["gh_labor"]["total staff"]
        args_gh_water["total area per week"] = inputs["total area per week"]
        result["gh_water"] = Gh_Water.compute(args_gh_water, quantize_output=True)
        s_start = timeit.default_timer()
        gas_cost_multiplier = fetch_one(
            "select multiplier from gas_cost_units where id=%(id)s", {
                'id': inputs['gas cost unit id']})['multiplier']
        result['sql_runtime'] += timeit.default_timer() - s_start
        gas_cost_dollars_per_thousand_cubic_feet = inputs['gas cost'] * \
            gas_cost_multiplier
        result['line items'] = [{
            'description': 'Electricity',
            'cost': result['gh_power']['total cost'],
            'cogs cost fraction': 0.8,
            'opex cost fraction': 0.2
        }, {
            'description': 'Water',
            'cost': result['gh_water']['total water'] * inputs['water cost'],
            'cogs cost fraction': 1,
            'opex cost fraction': 0
        }, {
            'description': 'Gas',
            'cost': result['gh_heating_opex']['natural gas required'] * gas_cost_dollars_per_thousand_cubic_feet / 1000,
            'cogs cost fraction': 0.8,
            'opex cost fraction': 0.2
        }, {
            'description': 'Rent',
            'cost': result['gh_rent']['total rent'],
            'cogs cost fraction': 0,
            'opex cost fraction': 1
        }, {
            'description': 'Labor',
            'cost': result['gh_labor']['total cost'],
            'cogs cost fraction': 0.8,
            'opex cost fraction': 0.2,
            'advanced': [
                {
                    "description": role['advanced name'],
                    "value": role['number of staff']
                } for role in result['gh_labor']['labor']
            ]
        }, {
            'description': 'Consumables',
            'cost': result['gh_cogp']['total cost'],
            'cogs cost fraction': 1,
            'opex cost fraction': 0
        }, {
            'description': 'Distribution',
            'cost': (advanced_inputs['distribution_rate']  * inputs['revenue'] / 100 + 500) if 'distribution_rate' in advanced_inputs else (0.015 * inputs['revenue'] + 500),
            'cogs cost fraction': 0,
            'opex cost fraction': 1,
            'advanced': [
                {
                    "description": 'distribution_rate',
                    "value": advanced_inputs['distribution_rate'] if 'distribution_rate' in advanced_inputs else 1.5
                }
            ]
        }, {
            'description': 'Depreciation',
            'cost': inputs['depreciation'],
            'cogs cost fraction': 0,
            'opex cost fraction': 0
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
