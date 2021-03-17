from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset, arr2dic
import math
import timeit
import json
from models.gh.input.land_status import Gh_Input_Land_Status
from models.gh.input.structure import Gh_Input_Structure
from models.gh.sizing.area import Gh_Sizing_Area
from models.gh.sizing.seeding import Gh_Sizing_Seeding
from models.gh.capex.grow import Gh_Grow
from models.gh.crop.characteristics import Gh_Crop_Characteristics
from models.gh.crop.crop import Gh_Crop
from models.gh.energy.energy import Gh_Energy
from models.gh.wastage.wastage import Gh_Wastage
from models.gh.design_elements.structure import Gh_Structure_Design_Elements
from models.gh.design_elements.covers import Gh_Covers_Design_Elements
from models.gh.capex.lighting import Gh_Lighting
from models.gh.capex.ventilation import Gh_Ventilation
from models.gh.opex.overview import Gh_Opex_Overview
from models.gh.capex.overview import Gh_Capex_Overview
from models.financials.interest import Financials_Interest
from models.financials.main import Financials_Main
from models.financials.employees import Financials_Employees
from models.financials.population_fed import Financials_Population_Fed


class Gh_Deliverables:

    @staticmethod
    def info():
        return {
            "name": "gh_deliverables",
            "description": "Deliverables for GH"
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
            "land status": {
                "type": "choice",
                "description": "Selection land status",
                "default": 1
            },
            "land cost": {
                "description": "Land cost per area ($ / sqft)",
                "type": "decimal"
            },
            "owner is headgrower": {
                "description": "Whether owner counts as headgrower",
                "type": "bool",
                "default": True
            },
            "headGrowerSalary": {
                "description": "Salary for head grower for an year",
                "type": "decimal",
                "default": 60000
            },
            "grower experience": {
                "type": "choice",
                "description": "Level of grower experience",
                "default": 1
            },
            "site area": {
                "description": "Total area to be allocated (sqft)",
                "type": "decimal"
            },
            "area fraction dedicated to production": {
                "description": "Fraction of main area to be allocated to bays (%, 0.5 to 0.85)",
                "default": 0.65,
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
                    "grow system type": {
                        "type": "choice",
                        "description": "Selection for grow system type",
                        "default": 1
                    }
                }
            },
            "structure type": {
                "type": "choice",
                "description": "Structure type",
                "default": 1
            },
            "organic production": {
                "description": "Whether the system will use organic production",
                "type": "bool",
                "default": False
            },
            "supplementary lighting": {
                "description": "Whether the system will include supplementary lighting",
                "type": "bool",
                "default": True
            },
            "heating": {
                "description": "Whether the system will include heating",
                "type": "bool",
                "default": True
            },
            "co2 injection": {
                "description": "Whether the system will include CO2 injection",
                "type": "bool",
                "default": True
            },
            "electricity cost": {
                "description": "Electricity cost ($ / kWh)",
                "default": 0.19,
                "type": "decimal"
            },
            "water cost": {
                "description": "Water cost ($ / gallon)",
                "default": 0.002,
                "type": "decimal"
            },
            "gas cost": {
                "description": "Gas cost",
                "default": 8.5,
                "type": "decimal"
            },
            "gas cost unit id": {
                "description": "Selected unit for gas cost",
                "default": 2,
                "type": "decimal"
            },
            "labor wages": {
                "description": "Labor wages ($ / h)",
                "default": 15.73,
                "type": "decimal"
            },
            "rent cost": {
                "description": "Rent cost per area ($ / sqft month)",
                "default": 12,
                "type": "decimal"
            },
            "tax rate": {
                "description": "Tax rate (%)",
                "default": 0.261,
                "type": "decimal"
            },
            "financing option": {
                "type": "choice",
                "description": "How capital expenses are financed",
                "default": 1
            },
            "interest rate": {
                "description": "Loan interest rate (% / y)",
                "default": 0.08,
                "type": "decimal"
            },
            "repayment time": {
                "description": "Repayment period for loan (y)",
                "default": 7,
                "type": "decimal"
            },
            "headhouse": {
                "description": "Enable/disable headhouse setting",
                "default": False,
                "type": "bool"
            },
            "circulation": {
                "description": "Enable/disable circulation setting",
                "default": False,
                "type": "bool"
            },
            "rooftop": {
                "description": "Enable/disable rooftop setting",
                "default": False,
                "type": "bool"
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
        if input_name == "land status":
            return Gh_Input_Land_Status.input_values("land status")
        if input_name == "grower experience":
            return Gh_Wastage.input_values("grower experience")
        if input_name == "crops.id":
            return fetch_all(
                "SELECT id, crop_type, reduced_level_height FROM crops WHERE is_gh_crop = 1 ORDER BY id")
        if input_name == "crops.sale unit id":
            return fetch_all(
                "SELECT id, description FROM sale_units ORDER BY id")
        if input_name == "crops.grow system type":
            return Gh_Grow.input_values("crops.grow system type")
        if input_name == "structure type":
            return Gh_Input_Structure.input_values("structure type")
        if input_name == "gas cost unit id":
            return Gh_Opex_Overview.input_values("gas cost unit id")
        if input_name == "financing option":
            return [{'id': 1, 'description': 'Debt'}, {
                'id': 2, 'description': 'Equity'}, {'id': 3, 'description': 'Self-funded'}]
        raise ValueError("Unexpected input: " + str(input_name))

    @staticmethod
    def output_type():
        return {
            "capex breakout": {
                "type": "list",
                "description": "Capex breakout",
                "element type": {
                    "category": {
                        "type": "text",
                        "description": "Category"
                    },
                    "total price": {
                        "description": "Total price ($)",
                        "digits": 2,
                        "type": "decimal"
                    },
                    "fraction": {
                        "description": "% of total price (%)",
                        "digits": 4,
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
            "cogs & opex breakout": {
                "type": "list",
                "description": "COGS & Opex breakout",
                "element type": {
                    "category": {
                        "type": "text",
                        "description": "Category"
                    },
                    "total price": {
                        "description": "Total price ($)",
                        "digits": 2,
                        "type": "decimal"
                    },
                    "fraction": {
                        "description": "% of total price (%)",
                        "digits": 4,
                        "type": "decimal"
                    }
                }
            },
            "cogs breakout": {
                "type": "list",
                "description": "COGS breakout",
                "element type": {
                    "category": {
                        "type": "text",
                        "description": "Category"
                    },
                    "total price": {
                        "description": "Total price ($)",
                        "digits": 2,
                        "type": "decimal"
                    }
                }
            },
            "opex breakout": {
                "type": "list",
                "description": "Opex breakout",
                "element type": {
                    "category": {
                        "type": "text",
                        "description": "Category"
                    },
                    "total price": {
                        "description": "Total price ($)",
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
            "max total yield": {
                "description": "Total Yield without Wastage (lbs)",
                "digits": 0,
                "type": "decimal"
            },
            "bedspace": {
                "description": "Total Bedspace needed (sqft)",
                "digits": 0,
                "type": "decimal"
            },
            "annual summary": {
                "type": "list",
                "description": "Annual summary",
                "element type": {
                    "year": {
                        "type": "integer",
                        "description": "Year"
                    },
                    "wastage": {
                        "description": "Wastage",
                        "digits": 4,
                        "type": "decimal"
                    },
                    "total yield": {
                        "description": "Total Yield (lbs)",
                        "digits": 0,
                        "type": "decimal"
                    },
                    "crop yield": {
                        "description": "Yield per each crop",
                        "type": "list",
                        "element type": {
                            "crop type": {
                                "type": "string",
                                "description": "crop name"
                            },
                            "yield" : {
                                "digits": 0,
                                "type": "decimal"
                            },
                            "even price" : {
                                "digits": 2,
                                "type": "decimal"
                            }
                        }
                    },
                    "waste-adjusted revenue": {
                        "description": "Waste-adjusted Revenue ($)",
                        "digits": 0,
                        "type": "decimal"
                    },
                    "cogs": {
                        "description": "COGS ($)",
                        "digits": 0,
                        "type": "decimal"
                    },
                    "opex": {
                        "description": "Opex ($)",
                        "digits": 0,
                        "type": "decimal"
                    },
                    "ebitda": {
                        "description": "EBITDA ($)",
                        "digits": 0,
                        "type": "decimal"
                    },
                    "depreciation & amortization": {
                        "description": "Depreciation & Amortization ($)",
                        "digits": 0,
                        "type": "decimal"
                    },
                    "interest payment": {
                        "description": "Interest Payment ($)",
                        "digits": 0,
                        "type": "decimal"
                    },
                    "taxes": {
                        "description": "Taxes ($)",
                        "digits": 0,
                        "type": "decimal"
                    },
                    "net profit": {
                        "description": "Net Profit ($)",
                        "digits": 0,
                        "type": "decimal"
                    }
                }
            },
            "cash flow": {
                "type": "list",
                "description": "Cash flow",
                "element type": {
                    "year": {
                        "type": "integer",
                        "description": "Year"
                    },
                    "net profit": {
                        "description": "Net Profit ($)",
                        "digits": 2,
                        "type": "decimal"
                    },
                    "d&a": {
                        "description": "Plus: D&A ($)",
                        "digits": 2,
                        "type": "decimal"
                    },
                    "net changes in working capital": {
                        "description": "Plus: Net Changes in Working Capital ($)",
                        "digits": 2,
                        "type": "decimal"
                    },
                    "free cash flow": {
                        "description": "Free Cash Flow ($)",
                        "digits": 2,
                        "type": "decimal"
                    },
                    "remaining balance": {
                        "description": "Remaining balance ($)",
                        "digits": 2,
                        "type": "decimal"
                    }
                }
            },
            "pv of cash flows": {
                "description": "PV of Cash Flows ($)",
                "digits": 2,
                "type": "decimal"
            },
            "payback period": {
                "description": "Payback period (y)",
                "digits": 2,
                "type": "decimal"
            },
            "financial summary": {
                "type": "list",
                "description": "Financial summary",
                "element type": {
                    "capex": {
                        "digits": 0,
                        "description": "Capex",
                        "type": "decimal"
                    },
                    "opex": {
                        "digits": 0,
                        "description": "Opex",
                        "type": "decimal"
                    },
                    "cogs": {
                        "digits": 0,
                        "description": "COGS",
                        "type": "decimal"
                    },
                    "opex + cogs": {
                        "digits": 0,
                        "description": "Opex + COGS",
                        "type": "decimal"
                    },
                    "max annual revenue": {
                        "digits": 0,
                        "description": "Max. Annual Revenue",
                        "type": "decimal"
                    },
                    "payback period": {
                        "digits": 2,
                        "description": "Payback Period (in years)",
                        "type": "decimal"
                    }
                }
            },
            "operating summary": {
                "type": "list",
                "description": "10-year operating summary",
                "element type": {
                    "year": {
                        "type": "integer",
                        "description": "Year"
                    },
                    "wastage": {
                        "digits": 4,
                        "description": "Wastage",
                        "type": "decimal"
                    },
                    "waste-adjusted revenue": {
                        "digits": 0,
                        "description": "Wastage-adjusted Revenue",
                        "type": "decimal"
                    },
                    "ebitda": {
                        "digits": 0,
                        "description": "EBITDA",
                        "type": "decimal"
                    },
                    "ebitda margin": {
                        "digits": 4,
                        "description": "EBITDA Margin",
                        "type": "decimal"
                    },
                    "net profit": {
                        "digits": 0,
                        "description": "Net Profit",
                        "type": "decimal"
                    },
                    "net margin": {
                        "digits": 4,
                        "description": "Net Margin",
                        "type": "decimal"
                    }
                }
            },
            "number of people employed": {
                "description": "Estimated number of people employed",
                "type": "text"
            },
            "potential population fed": {
                "description": "Estimated number of people fed",
                "digits": 0,
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
        inputs = parse_inputs(Gh_Deliverables.input_type(), args)
        advanced_inputs = arr2dic(inputs["advanced"])
        output_type = Gh_Deliverables.output_type()
        result['sql_runtime'] = 0
        print (inputs)
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        def print_debug(model_name):
            print(model_name, result[model_name])

        # First, translate inputs into land status and structure, cover
        # characteristics
        args_gh_input_land_status = dict()
        args_gh_input_land_status["land status"] = inputs["land status"]
        result["gh_input_land_status"] = Gh_Input_Land_Status.compute(
            args_gh_input_land_status, quantize_output=True)
        result['sql_runtime'] += result["gh_input_land_status"]['sql_runtime']

        print_debug('gh_input_land_status')
        args_gh_input_structure = dict()
        args_gh_input_structure["latitude"] = inputs["latitude"]
        args_gh_input_structure["longitude"] = inputs["longitude"]
        args_gh_input_structure["structure type"] = inputs["structure type"]
        result["gh_input_structure"] = Gh_Input_Structure.compute(
            args_gh_input_structure, quantize_output=True)
        result['sql_runtime'] += result["gh_input_structure"]['sql_runtime']

        print_debug('gh_input_structure')
        # Next, compute sizes for models
        args_gh_sizing_area = dict()
        args_gh_sizing_area["area"] = inputs["site area"]
        args_gh_sizing_area["headhouse_enabled"] = inputs["headhouse"]
        args_gh_sizing_area["circulation_enabled"] = inputs["circulation"]
        result["gh_sizing_area"] = Gh_Sizing_Area.compute(args_gh_sizing_area, quantize_output=True)
        result['sql_runtime'] += result["gh_sizing_area"]['sql_runtime']

        print_debug('gh_sizing_area')
        set_if_unset(
            result,
            "bedspace",
            result["gh_sizing_area"]["main bedspace"])
        # Compute characteristics for grow system and crop
        args_gh_grow = dict()
        args_gh_grow["bedspace"] = result["gh_sizing_area"]["main bedspace"]
        args_gh_grow["crops"] = inputs["crops"]
        if "grow_system_depreciation_duration" in advanced_inputs:
            args_gh_grow["depreciation duration"] = advanced_inputs["grow_system_depreciation_duration"]
        result["gh_grow"] = Gh_Grow.compute(args_gh_grow, quantize_output=True)
        result['sql_runtime'] += result["gh_grow"]['sql_runtime']

        print_debug('gh_grow')
        args_gh_crop_characteristics = dict()
        args_gh_crop_characteristics["crops"] = result["gh_grow"]["crops"]
        result["gh_crop_characteristics"] = Gh_Crop_Characteristics.compute(
            args_gh_crop_characteristics, quantize_output=True)
        result['sql_runtime'] += result["gh_crop_characteristics"]['sql_runtime']

        print_debug('gh_crop_characteristics')
        # Compute characteristics from (automatic) choice for structure and
        # covers
        args_gh_structure_design_elements = dict()
        args_gh_structure_design_elements["element_id"] = result["gh_input_structure"]["structure element_id"]
        args_gh_structure_design_elements["bays area"] = result["gh_sizing_area"]["bays area"]
        result["gh_structure_design_elements"] = Gh_Structure_Design_Elements.compute(
            args_gh_structure_design_elements, quantize_output=True)
        result['sql_runtime'] += result["gh_structure_design_elements"]['sql_runtime']

        print_debug('gh_structure_design_elements')
        args_gh_covers_design_elements = dict()
        args_gh_covers_design_elements["element_id"] = result["gh_input_structure"]["covers element_id"]
        result["gh_covers_design_elements"] = Gh_Covers_Design_Elements.compute(
            args_gh_covers_design_elements, quantize_output=True)
        result['sql_runtime'] += result["gh_covers_design_elements"]['sql_runtime']

        print_debug('gh_covers_design_elements')
        # Select lighting system based on sizing, crop lighting requirements
        # and electricity cost
        args_gh_lighting = dict()
        args_gh_lighting["bedspace"] = result["gh_sizing_area"]["main bedspace"]
        args_gh_lighting["ppfd"] = result["gh_crop_characteristics"]["max optimal avg ppfd"] if inputs["supplementary lighting"] else 0
        args_gh_lighting["electricity cost"] = inputs["electricity cost"]
        result["gh_lighting"] = Gh_Lighting.compute(args_gh_lighting, quantize_output=True)
        result['sql_runtime'] += result["gh_lighting"]['sql_runtime']

        print_debug('gh_lighting')
        # Compute energy calculations
        args_gh_energy = dict()
        args_gh_energy["latitude"] = inputs["latitude"]
        args_gh_energy["longitude"] = inputs["longitude"]
        args_gh_energy["area"] = result["gh_structure_design_elements"]["footprint"]
        args_gh_energy["volume"] = result["gh_structure_design_elements"]["volume"]
        args_gh_energy["cover to footprint ratio"] = result["gh_structure_design_elements"]["cover area"] / \
            result["gh_structure_design_elements"]["footprint"]
        args_gh_energy["mean height"] = result["gh_structure_design_elements"]["volume"] / \
            result["gh_structure_design_elements"]["footprint"]
        args_gh_energy["dli required"] = result["gh_crop_characteristics"]["avg optimal dli"]
        args_gh_energy["dli provided"] = result["gh_crop_characteristics"]["max optimal avg ppfd"] * 24 * 3600 / 1000000
        args_gh_energy["lai"] = result["gh_crop_characteristics"]["combined lai"]
        args_gh_energy["r_b"] = result["gh_crop_characteristics"]["combined r_b"]
        args_gh_energy["r_s_min"] = result["gh_crop_characteristics"]["combined r_s_min"]
        args_gh_energy["r_s_min_night"] = result["gh_crop_characteristics"]["combined r_s_min_night"]
        args_gh_energy["l_s"] = result["gh_crop_characteristics"]["combined l_s"]
        args_gh_energy["l_i"] = result["gh_crop_characteristics"]["combined l_i"]
        args_gh_energy["min crop temperature"] = result["gh_crop_characteristics"]["combined min crop temperature"]
        args_gh_energy["max crop temperature"] = result["gh_crop_characteristics"]["combined max crop temperature"]
        args_gh_energy["crop optimal cycle"] = result["gh_crop_characteristics"]["combined crop cycle"]
        args_gh_energy["crop degree days per dev cycle"] = result["gh_crop_characteristics"]["combined degree days per cycle"]
        args_gh_energy["crop optimal ppfd"] = result["gh_crop_characteristics"]["max optimal avg ppfd"]
        args_gh_energy["crop max unlimited assimilation"] = result["gh_crop_characteristics"]["combined max unlimited assimilation"]
        args_gh_energy["transmissivity"] = result["gh_covers_design_elements"]["transmissivity"]
        args_gh_energy["reflectivity"] = result["gh_covers_design_elements"]["reflectivity"]
        args_gh_energy["utr"] = result["gh_covers_design_elements"]["utr"]
        args_gh_energy["lighting power density"] = result["gh_lighting"]["power"] / \
            result["gh_sizing_area"]["main bedspace"]
        args_gh_energy["is heating"] = inputs["heating"]
        args_gh_energy["is lighting"] = inputs["supplementary lighting"]
        args_gh_energy["is co2 injection"] = inputs["co2 injection"]
        result["gh_energy"] = Gh_Energy.compute(args_gh_energy, quantize_output=True)
        result['sql_runtime'] += result["gh_energy"]['sql_runtime']

        print_debug('gh_energy')
        # Compute crop yield and revenue
        args_gh_crop = dict()
        args_gh_crop["bedspace"] = result["gh_sizing_area"]["main bedspace"]
        args_gh_crop["crops"] = result["gh_grow"]["crops"]
        args_gh_crop["yield impact coefficient"] = result["gh_energy"]["yield impact coefficient"]
        result["gh_crop"] = Gh_Crop.compute(args_gh_crop, quantize_output=True)
        result['sql_runtime'] += result["gh_crop"]['sql_runtime']

        print_debug('gh_crop')
        set_if_unset(
            result,
            "max total yield",
            result["gh_crop"]["total yield"])
        # Size up seeding area based on crop yield
        args_gh_sizing_seeding = dict()
        args_gh_sizing_seeding["annual number of plant sites"] = result["gh_crop"]["total plant sites"]
        args_gh_sizing_seeding["total substrate req"] = result["gh_crop"]["total substrate req"]
        result["gh_sizing_seeding"] = Gh_Sizing_Seeding.compute(
            args_gh_sizing_seeding, quantize_output=True)
        result['sql_runtime'] += result["gh_sizing_seeding"]['sql_runtime']

        print_debug('gh_sizing_seeding')
        # Compute wastage projection based on inputs and selected crops
        args_gh_wastage = dict()
        args_gh_wastage["grower experience"] = inputs["grower experience"]
        args_gh_wastage["organic production"] = inputs["organic production"]
        args_gh_wastage["crop base difficulty"] = result["gh_crop_characteristics"]["combined difficulty factor"]
        args_gh_wastage["number of crops"] = len(inputs["crops"])
        result["gh_wastage"] = Gh_Wastage.compute(args_gh_wastage, quantize_output=True)
        result['sql_runtime'] += result["gh_wastage"]['sql_runtime']

        print_debug('gh_wastage')
        # Choose ventilation system based on energy calculations
        args_gh_ventilation = dict()
        args_gh_ventilation["ventilation total cfm"] = result["gh_energy"]["ventilation total cfm"]
        args_gh_ventilation["ventilation time"] = result["gh_energy"]["ventilation time"]
        args_gh_ventilation["electricity cost"] = inputs["electricity cost"]
        result["gh_ventilation"] = Gh_Ventilation.compute(args_gh_ventilation, quantize_output=True)
        result['sql_runtime'] += result["gh_ventilation"]['sql_runtime']

        print_debug('gh_ventilation')
        # Compute capital expenditures
        args_gh_capex_overview = dict()
        args_gh_capex_overview["bedspace"] = result["gh_sizing_area"]["main bedspace"]
        args_gh_capex_overview["cold storage area fraction"] = result["gh_sizing_area"]["headhouse"][2]["percentage"]
        args_gh_capex_overview["bedspace seeding"] = result["gh_sizing_seeding"]["seeding bedspace needed non-microgreens"]
        args_gh_capex_overview["ppfd"] = result["gh_crop_characteristics"][
            "max optimal avg ppfd"] if inputs["supplementary lighting"] else 0
        args_gh_capex_overview["cooling rate"] = 60 if result["gh_energy"]["scenario"] == 4 else 0
        args_gh_capex_overview["co2 injection rate"] = 5 if inputs["co2 injection"] else 0
        args_gh_capex_overview["ventilation total cfm"] = result["gh_energy"]["ventilation total cfm"]
        args_gh_capex_overview["ventilation time"] = result["gh_energy"]["ventilation time"]
        args_gh_capex_overview["grow system cost"] = result["gh_grow"]["cost"]
        args_gh_capex_overview["grow system depreciation"] = result["gh_grow"]["depreciation"]
        args_gh_capex_overview["grow system depreciation duration"] = result["gh_grow"]["depreciation duration"]
        args_gh_capex_overview["real estate cost"] = inputs["site area"] * \
            inputs["land cost"] if result["gh_input_land_status"]["is buy"] else 0
        args_gh_capex_overview["amount harvested per week"] = result["gh_crop"]["total yield"] / 52
        args_gh_capex_overview["heating system size"] = result["gh_energy"]["heating system size"] if inputs["heating"] else 0
        args_gh_capex_overview["structure cost"] = result["gh_structure_design_elements"]["investment per area"] * \
            result["gh_sizing_area"]["bays area"]
        args_gh_capex_overview["structure depreciation"] = result["gh_structure_design_elements"]["operating cost"] * \
            result["gh_sizing_area"]["bays area"]
        args_gh_capex_overview["covers cost"] = result["gh_covers_design_elements"]["investment per area"] * \
            result["gh_sizing_area"]["bays area"]
        args_gh_capex_overview["covers depreciation"] = result["gh_covers_design_elements"]["operating cost"] * \
            result["gh_sizing_area"]["bays area"]
        args_gh_capex_overview["electricity cost"] = inputs["electricity cost"]
        args_gh_capex_overview["structure type"] = result["gh_structure_design_elements"]["element_id"]#inputs["structure type"]
        args_gh_capex_overview["bays area"] = result["gh_sizing_area"]["bays area"]
        args_gh_capex_overview["rooftop"] = inputs["rooftop"]
        args_gh_capex_overview["site area"] = inputs["site area"]
        args_gh_capex_overview["advanced"] = inputs["advanced"]
        result["gh_capex_overview"] = Gh_Capex_Overview.compute(
            args_gh_capex_overview, quantize_output=True)
        result['sql_runtime'] += result["gh_capex_overview"]['sql_runtime']

        print_debug('gh_capex_overview')
        # Compute operating expenses
        args_gh_opex_overview = dict()
        args_gh_opex_overview["bedspace"] = result["gh_sizing_area"]["main bedspace"]
        args_gh_opex_overview["rent area"] = inputs["site area"] if result["gh_input_land_status"]["is rent"] else 0
        args_gh_opex_overview["total area per week"] = result["gh_crop"]["total area per week"]
        args_gh_opex_overview["annual number of plant sites"] = result["gh_crop"]["total plant sites"]
        args_gh_opex_overview["total substrate req"] = result["gh_crop"]["total substrate req"]
        args_gh_opex_overview["sale units"] = result["gh_crop"]["total sale units"]
        args_gh_opex_overview["seed cost per site"] = result["gh_crop_characteristics"]["combined seed price"]
        args_gh_opex_overview["microgreens seed cost per site"] = result["gh_crop_characteristics"]["combined substrate cost"]
        args_gh_opex_overview["heating required"] = result["gh_energy"]["energy use heating"] if inputs["heating"] else 0
        args_gh_opex_overview["ventilation power"] = result["gh_ventilation"]["power"]
        args_gh_opex_overview["lighting power density"] = result["gh_lighting"]["power"] / \
            result["gh_sizing_area"]["main bedspace"]
        args_gh_opex_overview["irrigation water"] = result["gh_energy"]["water transpired"] * 1.1
        args_gh_opex_overview["electricity cost"] = inputs["electricity cost"]
        args_gh_opex_overview["water cost"] = inputs["water cost"]
        args_gh_opex_overview["gas cost"] = inputs["gas cost"]
        args_gh_opex_overview["gas cost unit id"] = inputs["gas cost unit id"]
        args_gh_opex_overview["labor wages"] = inputs["labor wages"]
        args_gh_opex_overview["rent cost"] = 12 * inputs["rent cost"]
        args_gh_opex_overview["automation level"] = result["gh_capex_overview"]["automation level"]
        args_gh_opex_overview["owner is headgrower"] = inputs["owner is headgrower"]
        args_gh_opex_overview["headgrower salary"] = inputs["headGrowerSalary"]
        args_gh_opex_overview["packaging type"] = 1
        args_gh_opex_overview["revenue"] = result["gh_crop"]["total revenue"] * \
            (1 - result["gh_wastage"]["initial wastage"])
        args_gh_opex_overview["depreciation"] = result["gh_capex_overview"]["total depreciation"]
        args_gh_opex_overview["lighting hours per year"] = result["gh_energy"]["lighting time"]
        args_gh_opex_overview["advanced"] = inputs["advanced"]
        result["gh_opex_overview"] = Gh_Opex_Overview.compute(
            args_gh_opex_overview, quantize_output=True)
        result['sql_runtime'] += result["gh_opex_overview"]['sql_runtime']

        print_debug('gh_opex_overview')
        # Compute repayment schedule for debt
        # Financing amount is 0 if funding option is not debt
        args_financials_interest = dict()
        args_financials_interest["amount"] = result["gh_capex_overview"]["total cost"] if inputs["financing option"] == 1 else 0
        args_financials_interest["interest rate"] = inputs["interest rate"]
        args_financials_interest["repayment time"] = inputs["repayment time"]
        args_financials_interest["payments per year"] = 4
        result["financials_interest"] = Financials_Interest.compute(
            args_financials_interest, quantize_output=True)
        result['sql_runtime'] += result["financials_interest"]['sql_runtime']

        print_debug('financials_interest')
        # Combine outputs into financial model
        args_financials_main = dict()
        args_financials_main["capex line items"] = result["gh_capex_overview"]["line items"]
        args_financials_main["opex line items"] = result["gh_opex_overview"]["line items"]
        args_financials_main["wastage schedule"] = result["gh_wastage"]["wastage schedule"]
        args_financials_main["crops"] = result["gh_crop"]["crops"]
        args_financials_main["base yield"] = result["gh_crop"]["total yield"]
        args_financials_main["base revenue"] = result["gh_crop"]["total revenue"]
        args_financials_main["depreciation"] = result["gh_capex_overview"]["total depreciation"]
        args_financials_main["interest repayment schedule"] = result["financials_interest"]["repayment schedule"]
        args_financials_main["tax rate"] = inputs["tax rate"]
        result["financials_main"] = Financials_Main.compute(
            args_financials_main, quantize_output=True)
        result['sql_runtime'] += result["financials_main"]['sql_runtime']

        print_debug('financials_main')
        # Output number of employees
        args_financials_employees = dict()
        args_financials_employees["total staff"] = result["gh_opex_overview"]["total staff"]
        result["financials_employees"] = Financials_Employees.compute(
            args_financials_employees, quantize_output=True)
        result['sql_runtime'] += result["financials_employees"]['sql_runtime']

        print_debug('financials_employees')
        # Output population fed
        args_financials_population_fed = dict()
        args_financials_population_fed["total yield"] = result["gh_crop"]["total yield"]
        result["financials_population_fed"] = Financials_Population_Fed.compute(
            args_financials_population_fed, quantize_output=True)
        result['sql_runtime'] += result["financials_population_fed"]['sql_runtime']

        print_debug('financials_population_fed')
        # Output financial model results
        set_if_unset(
            result,
            "number of people employed",
            result["financials_employees"]["number of people employed"])
        set_if_unset(
            result,
            "potential population fed",
            result["financials_population_fed"]["potential population fed"])
        set_if_unset(
            result,
            "capex breakout",
            result["financials_main"]["capex breakout"])
        # add depreciation
        for i, item in enumerate(args_financials_main["capex line items"]):
            result["capex breakout"][i]["depreciation"] = item["depreciation"]
            if "advanced" in item:
                result["capex breakout"][i]["advanced"] = item["advanced"]
        set_if_unset(
            result,
            "cogs & opex breakout",
            result["financials_main"]["cogs & opex breakout"])
        set_if_unset(
            result,
            "cogs breakout",
            result["financials_main"]["cogs breakout"])
        set_if_unset(
            result,
            "opex breakout",
            result["financials_main"]["opex breakout"])
        for i, item in enumerate(args_financials_main["opex line items"]):
            if "advanced" in item:
                result["opex breakout"][i]["advanced"] = item["advanced"]
        set_if_unset(
            result,
            "annual summary",
            result["financials_main"]["annual summary"])
        set_if_unset(
            result,
            "cash flow",
            result["financials_main"]["cash flow"])
        set_if_unset(
            result,
            "pv of cash flows",
            result["financials_main"]["pv of cash flows"])
        set_if_unset(
            result,
            "payback period",
            result["financials_main"]["payback period"])
        set_if_unset(
            result,
            "financial summary",
            result["financials_main"]["financial summary"])
        set_if_unset(
            result,
            "operating summary",
            result["financials_main"]["operating summary"])
        result['runtime'] = timeit.default_timer() - start
        if quantize_output:
            quantize_outputs(output_type, result)
        print(result)
        return result