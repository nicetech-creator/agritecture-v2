from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset, arr2dic
import math
import sys
from models.gh.capex.lighting import Gh_Lighting
from models.gh.capex.seeding_germination import Gh_Seeding_Germination_Capex
from models.gh.capex.propagation import Gh_Propagation_Capex
from models.gh.capex.processing import Gh_Processing_Capex
from models.gh.capex.cold_storage import Gh_Cold_Storage_Capex
from models.gh.capex.heating import Gh_Heating
from models.gh.capex.cooling import Gh_Cooling
from models.gh.capex.ventilation import Gh_Ventilation
from models.gh.capex.co2_injection import Gh_Co2_Injection
from models.gh.input.structure import Gh_Input_Structure
import timeit

class Gh_Capex_Overview:

    @staticmethod
    def info():
        return {
            "name": "gh_capex_overview",
            "description": "Overview of Capex costs for GH"
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
            "cooling rate": {
                "description": "Selected cooling rate (exchanges / min)",
                "type": "decimal"
            },
            "ventilation total cfm": {
                "description": "Total ventilation rate in cubic feet per minute (CFM)",
                "type": "decimal"
            },
            "ventilation time": {
                "description": "Time ventilation is active in hours per year (h / y)",
                "type": "decimal"
            },
            "co2 injection rate": {
                "description": "Selected CO2 injection rate (mg / m2 s)",
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
            "amount harvested per week": {
                "description": "Amount harvested per week (lb / w)",
                "type": "decimal"
            },
            "heating system size": {
                "description": "Size of heating system (kW)",
                "type": "decimal"
            },
            "structure cost": {
                "description": "Cost used in structures ($)",
                "type": "decimal"
            },
            "covers cost": {
                "description": "Cost used in covers ($)",
                "type": "decimal"
            },
            "structure depreciation": {
                "description": "Depreciation used in structures ($ / y)",
                "type": "decimal"
            },
            "covers depreciation": {
                "description": "Depreciation used in structures ($ / y)",
                "type": "decimal"
            },
            "electricity cost": {
                "description": "Cost of electricity ($ / kWh)",
                "type": "decimal"
            },
            "structure type": {
                "type": "choice",
                "description": "Structure type",
                "default": 1
            },
            "bays area": {
                "description": "Area allocated to bays (sqft)",
                "type": "decimal"
            },
            "rooftop": {
                "description": "Enable/disable rooftop setting",
                "default": False,
                "type": "bool"
            },
            "site area": {
                "description": "Total area to be allocated (sqft)",
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
        if input_name == "structure type":
            return Gh_Input_Structure.input_values("structure type")
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
                "description": "Total operating expenses cost ($)",
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
        inputs = parse_inputs(Gh_Capex_Overview.input_type(), args)
        advanced_inputs = arr2dic(inputs["advanced"])
        output_type = Gh_Capex_Overview.output_type()
        result['sql_runtime'] = 0
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        # Supplemental lights disabled when ppfd is 0
        args_gh_lighting = dict()
        args_gh_lighting["bedspace"] = inputs["bedspace"]
        args_gh_lighting["ppfd"] = inputs["ppfd"]
        args_gh_lighting["electricity cost"] = inputs["electricity cost"]
        if "lighting_depreciation_duration" in advanced_inputs:
            args_gh_lighting["depreciation duration"] = advanced_inputs["lighting_depreciation_duration"]
        result["gh_lighting"] = Gh_Lighting.compute(args_gh_lighting, quantize_output=True)
        result['sql_runtime'] += result["gh_lighting"]['sql_runtime']

        args_gh_heating = dict()
        args_gh_heating["heating system size"] = inputs["heating system size"]
        result["gh_heating"] = Gh_Heating.compute(args_gh_heating, quantize_output=True)
        result['sql_runtime'] += result["gh_heating"]['sql_runtime']

        args_gh_cooling = dict()
        args_gh_cooling["area"] = inputs["bedspace"]
        args_gh_cooling["cooling rate"] = inputs["cooling rate"]
        result["gh_cooling"] = Gh_Cooling.compute(args_gh_cooling, quantize_output=True)
        result['sql_runtime'] += result["gh_cooling"]['sql_runtime']

        # Mechanical ventilation disabled when ventilation time is 0 or cfm is
        # 0
        args_gh_ventilation = dict()
        args_gh_ventilation["electricity cost"] = inputs["electricity cost"]
        args_gh_ventilation["ventilation total cfm"] = inputs["ventilation total cfm"]
        args_gh_ventilation["ventilation time"] = inputs["ventilation time"]
        result["gh_ventilation"] = Gh_Ventilation.compute(args_gh_ventilation, quantize_output=True)
        result['sql_runtime'] += result["gh_ventilation"]['sql_runtime']

        # CO2 injection disabled when rate is 0
        args_gh_co2_injection = dict()
        args_gh_co2_injection["injection rate"] = inputs["co2 injection rate"]
        args_gh_co2_injection["bedspace"] = inputs["bedspace"]
        result["gh_co2_injection"] = Gh_Co2_Injection.compute(
            args_gh_co2_injection, quantize_output=True)
        result['sql_runtime'] += result["gh_co2_injection"]['sql_runtime']

        args_gh_seeding_germination_capex = dict()
        args_gh_seeding_germination_capex["main bedspace"] = inputs["bedspace"]
        args_gh_seeding_germination_capex["bedspace seeding"] = inputs["bedspace seeding"]
        if "seeding_depreciation_duration" in advanced_inputs:
            args_gh_seeding_germination_capex["depreciation duration"] = advanced_inputs['seeding_depreciation_duration']
        result["gh_seeding_germination_capex"] = Gh_Seeding_Germination_Capex.compute(
            args_gh_seeding_germination_capex, quantize_output=True)
        result['sql_runtime'] += result["gh_seeding_germination_capex"]['sql_runtime']

        args_gh_propagation_capex = dict()
        if "propagation_depreciation_duration" in advanced_inputs:
            args_gh_propagation_capex["depreciation duration"] = advanced_inputs["propagation_depreciation_duration"]
        result["gh_propagation_capex"] = Gh_Propagation_Capex.compute(
            args_gh_propagation_capex, quantize_output=True)
        result['sql_runtime'] += result["gh_propagation_capex"]['sql_runtime']

        args_gh_processing_capex = dict()
        args_gh_processing_capex["amount harvested per week"] = inputs["amount harvested per week"]
        if "processing_depreciation_duration" in advanced_inputs:
            args_gh_processing_capex['depreciation duration'] = advanced_inputs["processing_depreciation_duration"]
        result["gh_processing_capex"] = Gh_Processing_Capex.compute(
            args_gh_processing_capex, quantize_output=True)
        result['sql_runtime'] += result["gh_processing_capex"]['sql_runtime']

        args_gh_cold_storage_capex = dict()
        args_gh_cold_storage_capex["bedspace"] = inputs["bedspace"]
        args_gh_cold_storage_capex["cold storage area fraction"] = inputs["cold storage area fraction"]
        if "cold_storage_depreciation_duration" in advanced_inputs:
            args_gh_cold_storage_capex["depreciation duration"] = advanced_inputs["cold_storage_depreciation_duration"]
        result["gh_cold_storage_capex"] = Gh_Cold_Storage_Capex.compute(
            args_gh_cold_storage_capex, quantize_output=True)
        result['sql_runtime'] += result["gh_cold_storage_capex"]['sql_runtime']
        
        # construction_cost_map = {1: 15, 2: 12, 3: 7}
        # construction_cost = inputs['bays area'] * \
        #     construction_cost_map[inputs['structure type']]

        # construction cost with size factors
        structure_type_map = {1: "LD1", 2: "LD2", 3: "MD", 4: "HD"}
        construction_cost_factor_sql = 'select construct_cost from gh_design_elements_structure where type=%(type)s'
        s_start = timeit.default_timer()
        construction_cost_factor_row = fetch_one(construction_cost_factor_sql, {'type': structure_type_map[inputs['structure type']]})

        struct_cost_factor_sql = 'select size, factor from struct_cost where type=%(type)s'
        struct_cost_factor_rows = fetch_all(struct_cost_factor_sql, {'type': structure_type_map[inputs['structure type']]})
        result['sql_runtime'] += timeit.default_timer() - s_start
        size = sys.maxsize
        factor = 0
        inf_factor = 0
        for row in struct_cost_factor_rows:
            if row['size'] == 'inf': inf_factor = row['factor']
            else:
                s = int(row['size'])
                if inputs['bays area'] <= s and s <= size:
                    size = s
                    factor = row['factor']
        if size == sys.maxsize:
            factor = inf_factor
        construction_cost = inputs['bays area'] * construction_cost_factor_row['construct_cost'] * factor

        # additional cost when rooftop enabled
        if inputs['rooftop'] == True:
            s_start = timeit.default_timer()
            rows = fetch_all('select * from gh_rooftop_addition')
            result['sql_runtime'] += timeit.default_timer() - s_start
            size = sys.maxsize
            factor1 = 0
            for row in rows:
                if inputs['site area'] < row['site_area'] and row['site_area'] <= size:
                    size = row['site_area']
                    factor1 = row['design_fee'] + row['structural_improves']
            construction_cost += inputs['site area'] * factor1
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
            'cost': result['gh_lighting']['cost'],
            'depreciation': result['gh_lighting']['depreciation'],
            'advanced': [
                {
                    'description': 'lighting_depreciation_duration',
                    'value': result['gh_lighting']['depreciation duration']
                }
            ]
        }, {
            'description': 'Heating',
            'cost': result['gh_heating']['cost'],
            'depreciation': result['gh_heating']['depreciation']
        }, {
            'description': 'Cooling',
            'cost': result['gh_cooling']['cost'] + result['gh_ventilation']['cost'],
            'depreciation': result['gh_cooling']['depreciation'] + result['gh_ventilation']['depreciation']
        }, {
            'description': 'CO2 Injection',
            'cost': result['gh_co2_injection']['cost'],
            'depreciation': result['gh_co2_injection']['depreciation']
        }, {
            'description': 'Seeding',
            'cost': result['gh_seeding_germination_capex']['cost'],
            'depreciation': result['gh_seeding_germination_capex']['depreciation'],
            'advanced': [
                {
                    'description': 'seeding_depreciation_duration',
                    'value': result['gh_seeding_germination_capex']['depreciation duration']
                }
            ]
        }, {
            'description': 'Propagation',
            'cost': result['gh_propagation_capex']['cost'],
            'depreciation': result['gh_propagation_capex']['depreciation'],
            'advanced': [
                {
                    'description': 'propagation_depreciation_duration',
                    'value': result['gh_propagation_capex']['depreciation duration']
                }
            ]
        }, {
            'description': 'Processing',
            'cost': result['gh_processing_capex']['cost'],
            'depreciation': result['gh_processing_capex']['depreciation'],
            'advanced': [
                {
                    'description': 'processing_depreciation_duration',
                    'value': result['gh_processing_capex']['depreciation duration']
                }
            ]
        }, {
            'description': 'Cold Storage',
            'cost': result['gh_cold_storage_capex']['cost'],
            'depreciation': result['gh_cold_storage_capex']['depreciation'],
            'advanced': [
                {
                    'description': 'cold_storage_depreciation_duration',
                    'value': result['gh_cold_storage_capex']['depreciation duration']
                }
            ]
        }, {
            'description': 'Real Estate',
            'cost': inputs['real estate cost'],
            'depreciation': 0
        }, {
            'description': 'Structures',
            'cost': (inputs['structure cost'] + inputs['covers cost']) * factor,
            'depreciation': (inputs['structure depreciation'] + inputs['covers depreciation']) * factor
        }, {
            'description': 'Construction',
            'cost': construction_cost,
            'depreciation': 0
        }]
        result['equipment selection'] = [{
            'type': 'Lighting',
            'description': result['gh_lighting']['description'],
            'units': result['gh_lighting']['number of units']
        }, {
            'type': 'Heating',
            'description': result['gh_heating']['description'],
            'units': result['gh_heating']['number of units']
        }, {
            'type': 'Cooling',
            'description': result['gh_cooling']['description'],
            'units': 1
        }, {
            'type': 'Ventilation',
            'description': result['gh_ventilation']['description'],
            'units': result['gh_ventilation']['number of units']
        }, {
            'type': 'CO2 Injection',
            'description': result['gh_co2_injection']['description'],
            'units': result['gh_co2_injection']['number of units']
        }, {
            'type': 'Seeding',
            'description': result['gh_seeding_germination_capex']['description'],
            'units': result['gh_seeding_germination_capex']['number of units']
        }, {
            'type': 'Harvester',
            'description': result['gh_processing_capex']['harvester description'],
            'units': result['gh_processing_capex']['number of harvesters']
        }, {
            'type': 'Washer',
            'description': result['gh_processing_capex']['washer description'],
            'units': result['gh_processing_capex']['number of washers']
        }]

        set_if_unset(
            result, "total cost", sum(
                line["cost"] for line in result["line items"]))
        set_if_unset(result, "total depreciation", sum(
            line["depreciation"] for line in result["line items"]))
        # Compute combined automation level
        seeder_automation_level = result["gh_seeding_germination_capex"]["automation level"]
        harvester_automation_level = result["gh_processing_capex"]["harvester automation level"]
        washer_automation_level = result["gh_processing_capex"]["washer automation level"]
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