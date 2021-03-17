from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset, arr2dic
import math
import timeit

from models.vf.capex.hvac import Vf_Hvac
from models.vf.capex.racking import Vf_Racking
from models.vf.capex.lighting import Vf_Lighting
from models.vf.capex.seeding_germination import Vf_Seeding_Germination_Capex
from models.vf.capex.propagation import Vf_Propagation_Capex
from models.vf.capex.processing import Vf_Processing_Capex
from models.vf.capex.cold_storage import Vf_Cold_Storage_Capex


class Vf_Capex_Overview:

    @staticmethod
    def info():
        return {
            "name": "vf_capex_overview",
            "description": "Overview of Capex costs for VF"
        }

    @staticmethod
    def input_type():
        return {
            "bedspace": {
                "description": "Total crop bedspace (sqft)",
                "type": "decimal"
            },
            "cold storage area fraction": {
                "description": "Fraction of area reserved for cold storage (%)",
                "default": 0.015,
                "type": "decimal"
            },
            "bedspace seeding": {
                "description": "Bedspace area reserved for seeding, non-microgreens only (sqft)",
                "type": "decimal"
            },
            "ppfd": {
                "description": "Lighting system average PPFD (umol/m2 s)",
                "type": "decimal"
            },
            "tons of cooling": {
                "description": "Total cooling (tons of cooling)",
                "type": "decimal"
            },
            "number of levels": {
                "description": "Actual number of levels in system",
                "type": "decimal"
            },
            "grow system cost": {
                "description": "Cost used in the grow system ($)",
                "type": "decimal"
            },
            "grow system depreciation": {
                "description": "Depreciation used in the grow system ($ / y)",
                "type": "decimal"
            },
            "grow system depreciation duration": {
                "description": "Duration of item depreciation",
                "default": 15,
                "type": "decimal"
            },
            "real estate cost": {
                "description": "Cost associated with real estate ($)",
                "type": "decimal"
            },
            "building renovation cost": {
                "description": "Costs associated with building renovation ($)",
                "type": "decimal"
            },
            "amount harvested per week": {
                "description": "Amount harvested per week (lb / w)",
                "type": "decimal"
            },
            "electricity cost": {
                "description": "Cost of electricity ($ / kWh)",
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
                "description": "Total captial expenses cost ($)",
                "digits": 2,
                "type": "decimal"
            },
            "total depreciation": {
                "description": "Total depreciation per year ($ / y)",
                "digits": 2,
                "type": "decimal"
            },
            "equipment selection": {
                "type": "list",
                "description": "List of equipment",
                "element type": {
                    "type": {
                        "type": "text",
                        "description": "Equipment type"
                    },
                    "description": {
                        "type": "text",
                        "description": "Selected equipment"
                    },
                    "units": {
                        "type": "integer",
                        "description": "Number of units"
                    }
                }
            },
            "automation level": {
                "description": "Equipment automation level",
                "type": "text"
            },
            "hvac seer": {
                "description": "HVAC system energy efficiency rating",
                "digits": 4,
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
        inputs = parse_inputs(Vf_Capex_Overview.input_type(), args)
        advanced_inputs = arr2dic(inputs["advanced"])
        output_type = Vf_Capex_Overview.output_type()
        result['sql_runtime'] = 0
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        args_vf_lighting = dict()
        args_vf_lighting["bedspace"] = inputs["bedspace"]
        args_vf_lighting["electricity cost"] = inputs["electricity cost"]
        args_vf_lighting["ppfd"] = inputs["ppfd"]
        if "lighting_depreciation_duration" in advanced_inputs:
            args_vf_lighting["depreciation duration"] = advanced_inputs["lighting_depreciation_duration"]
        result["vf_lighting"] = Vf_Lighting.compute(args_vf_lighting, quantize_output=True)
        result['sql_runtime'] += result["vf_lighting"]['sql_runtime']

        args_vf_hvac = dict()
        args_vf_hvac["tons of cooling"] = inputs["tons of cooling"]
        if "hvac_depreciation_duration" in advanced_inputs:
            args_vf_hvac["depreciation duration"] = advanced_inputs["hvac_depreciation_duration"]
        result["vf_hvac"] = Vf_Hvac.compute(args_vf_hvac, quantize_output=True)
        result['sql_runtime'] += result["vf_hvac"]['sql_runtime']


        set_if_unset(result, "hvac seer", result["vf_hvac"]["seer"])
        args_vf_racking = dict()
        args_vf_racking["number of levels"] = inputs["number of levels"]
        args_vf_racking["bedspace"] = inputs["bedspace"]
        if "racking_depreciation_duration" in advanced_inputs:
            args_vf_racking["depreciation duration"] = advanced_inputs["racking_depreciation_duration"]
        if "racking_price_per_sft" in advanced_inputs:
            args_vf_racking["price_per_sft"] = advanced_inputs["racking_price_per_sft"] 
        result["vf_racking"] = Vf_Racking.compute(args_vf_racking, quantize_output=True)
        result['sql_runtime'] += result["vf_racking"]['sql_runtime']

        args_vf_seeding_germination_capex = dict()
        args_vf_seeding_germination_capex["main bedspace"] = inputs["bedspace"]
        args_vf_seeding_germination_capex["bedspace seeding"] = inputs["bedspace seeding"]
        if "seeding_depreciation_duration" in advanced_inputs:
            args_vf_seeding_germination_capex["depreciation duration"] = advanced_inputs['seeding_depreciation_duration']
        result["vf_seeding_germination_capex"] = Vf_Seeding_Germination_Capex.compute(
            args_vf_seeding_germination_capex, quantize_output=True)
        result['sql_runtime'] += result["vf_seeding_germination_capex"]['sql_runtime']

        args_vf_propagation_capex = dict()
        if "propagation_depreciation_duration" in advanced_inputs:
            args_vf_propagation_capex["depreciation duration"] = advanced_inputs["propagation_depreciation_duration"]
        result["vf_propagation_capex"] = Vf_Propagation_Capex.compute(
            args_vf_propagation_capex, quantize_output=True)

        args_vf_processing_capex = dict()
        args_vf_processing_capex["amount harvested per week"] = inputs["amount harvested per week"]
        if "processing_depreciation_duration" in advanced_inputs:
            args_vf_processing_capex['depreciation duration'] = advanced_inputs["processing_depreciation_duration"]
        result["vf_processing_capex"] = Vf_Processing_Capex.compute(
            args_vf_processing_capex, quantize_output=True)
        result['sql_runtime'] += result["vf_processing_capex"]['sql_runtime']

        args_vf_cold_storage_capex = dict()
        args_vf_cold_storage_capex["bedspace"] = inputs["bedspace"]
        args_vf_cold_storage_capex["cold storage area fraction"] = inputs["cold storage area fraction"]
        if "cold_storage_depreciation_duration" in advanced_inputs:
            args_vf_cold_storage_capex["depreciation duration"] = advanced_inputs["cold_storage_depreciation_duration"]
        result["vf_cold_storage_capex"] = Vf_Cold_Storage_Capex.compute(
            args_vf_cold_storage_capex, quantize_output=True)

        result['line items'] = [{
            'description': 'Grow System',
            'cost': inputs['grow system cost'],
            'depreciation': inputs['grow system depreciation'],
            'advanced': [
                {
                    'description': 'grow_system_depreciation_duration',
                    'value': inputs['grow system depreciation duration']
                }
            ]
        }, {
            'description': 'Lighting',
            'cost': result['vf_lighting']['cost'],
            'depreciation': result['vf_lighting']['depreciation'],
            'advanced': [
                {
                    'description': 'lighting_depreciation_duration',
                    'value': result['vf_lighting']['depreciation duration']
                }
            ]
        }, {
            'description': 'HVAC',
            'cost': result['vf_hvac']['cost'],
            'depreciation': result['vf_hvac']['depreciation'],
            'advanced': [
                {
                    'description': 'hvac_depreciation_duration',
                    'value': result['vf_hvac']['depreciation duration']
                }
            ]

        }, {
            'description': 'Racking',
            'cost': result['vf_racking']['cost'],
            'depreciation': result['vf_racking']['depreciation'],
            'advanced': [
                {
                    'description': 'racking_depreciation_duration',
                    'value': result['vf_racking']['depreciation duration']
                },
                {
                    'description': 'racking_price_per_sft',
                    'value': result['vf_racking']['price_per_sft']
                }
            ]
        }, {
            'description': 'Seeding',
            'cost': result['vf_seeding_germination_capex']['cost'],
            'depreciation': result['vf_seeding_germination_capex']['depreciation'],
            'advanced': [
                {
                    'description': 'seeding_depreciation_duration',
                    'value': result['vf_seeding_germination_capex']['depreciation duration']
                }
            ]
        }, {
            'description': 'Propagation',
            'cost': result['vf_propagation_capex']['cost'],
            'depreciation': result['vf_propagation_capex']['depreciation'],
            'advanced': [
                {
                    'description': 'propagation_depreciation_duration',
                    'value': result['vf_propagation_capex']['depreciation duration']
                }
            ]
        }, {
            'description': 'Processing',
            'cost': result['vf_processing_capex']['cost'],
            'depreciation': result['vf_processing_capex']['depreciation'],
            'advanced': [
                {
                    'description': 'processing_depreciation_duration',
                    'value': result['vf_processing_capex']['depreciation duration']
                }
            ]
        }, {
            'description': 'Cold Storage',
            'cost': result['vf_cold_storage_capex']['cost'],
            'depreciation': result['vf_cold_storage_capex']['depreciation'],
            'advanced': [
                {
                    'description': 'cold_storage_depreciation_duration',
                    'value': result['vf_cold_storage_capex']['depreciation duration']
                }
            ]
        }, {
            'description': 'Real Estate',
            'cost': inputs['real estate cost'],
            'depreciation': 0
        }, {
            'description': 'Building Renovations',
            'cost': inputs['building renovation cost'],
            'depreciation': 0
        }]
        result['equipment selection'] = [{
            'type': 'Lighting',
            'description': result['vf_lighting']['description'],
            'units':result['vf_lighting']['number of units']
        }, {
            'type': 'HVAC',
            'description': result['vf_hvac']['description'],
            'units': result['vf_hvac']['number of units']
        }, {
            'type': 'Racking',
            'description': result['vf_racking']['description'],
            'units': 1 if result['vf_racking']['cost'] > 0 else 0
        }, {
            'type': 'Seeding',
            'description': result['vf_seeding_germination_capex']['description'],
            'units': result['vf_seeding_germination_capex']['number of units']
        }, {
            'type': 'Harvester',
            'description': result['vf_processing_capex']['harvester description'],
            'units': result['vf_processing_capex']['number of harvesters']
        }, {
            'type': 'Washer',
            'description': result['vf_processing_capex']['washer description'],
            'units': result['vf_processing_capex']['number of washers']
        }]

        set_if_unset(
            result, "total cost", sum(
                line["cost"] for line in result["line items"]))
        set_if_unset(result, "total depreciation", sum(
            line["depreciation"] for line in result["line items"]))
        # Compute combined automation level
        seeder_automation_level = result["vf_seeding_germination_capex"]["automation level"]
        harvester_automation_level = result["vf_processing_capex"]["harvester automation level"]
        washer_automation_level = result["vf_processing_capex"]["washer automation level"]
        set_if_unset(
            result,
            "automation level",
            seeder_automation_level +
            harvester_automation_level +
            washer_automation_level)

        result['runtime'] = timeit.default_timer() - start
        if quantize_output:
            quantize_outputs(output_type, result)

        return result