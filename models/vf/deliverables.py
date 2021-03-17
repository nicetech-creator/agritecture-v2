from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset, arr2dic
import math
import timeit

from models.vf.input.land_status import Vf_Input_Land_Status
from models.vf.sizing.area import Vf_Sizing_Area
from models.vf.sizing.seeding import Vf_Sizing_Seeding
from models.vf.crop.characteristics import Vf_Crop_Characteristics
from models.vf.crop.crop import Vf_Crop
from models.vf.energy.roof import Vf_Roof
from models.vf.energy.setpoint_selection import Vf_Energy_Setpoint_Selection
from models.vf.energy.energy import Vf_Energy
from models.vf.wastage.wastage import Vf_Wastage
from models.vf.capex.lighting import Vf_Lighting
from models.vf.capex.grow import Vf_Grow
from models.vf.capex.overview import Vf_Capex_Overview
from models.vf.opex.overview import Vf_Opex_Overview
from models.financials.interest import Financials_Interest
from models.financials.main import Financials_Main
from models.financials.employees import Financials_Employees
from models.financials.population_fed import Financials_Population_Fed


class Vf_Deliverables:

    @staticmethod
    def info():
        return {
            "name": "vf_deliverables",
            "description": "Deliverables for VF"
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
                "description": "Salary for headgrower ($ / y)",
                "default": 60000,
                "type": "decimal"
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
            "height": {
                "description": "Height of the terrain (ft)",
                "type": "decimal"
            },
            "number of levels": {
                "description": "Desired number of levels",
                "type": "decimal"
            },
            "building insulation": {
                "type": "choice",
                "description": "Building insulation type",
                "default": 1
            },
            "building roof type": {
                "type": "choice",
                "description": "Building roof type",
                "default": 1
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
            "organic production": {
                "description": "Whether the system will use organic production",
                "type": "bool",
                "default": False
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
            "processing": {
                "description": "Enable/disable processing setting",
                "type": "bool",
                "default": False
            },
            "cold storage": {
                "description": "Enable/disable cold storage setting",
                "type": "bool",
                "default": False
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
            return Vf_Input_Land_Status.input_values("land status")
        if input_name == "grower experience":
            return Vf_Wastage.input_values("grower experience")
        if input_name == "building insulation":
            return Vf_Roof.input_values("building insulation")
        if input_name == "building roof type":
            return Vf_Roof.input_values("building roof type")
        if input_name == "crops.id":
            return fetch_all(
                "SELECT id, crop_type, reduced_level_height FROM crops WHERE is_vf_crop = 1 ORDER BY id")
        if input_name == "crops.sale unit id":
            return fetch_all(
                "SELECT id, description FROM sale_units ORDER BY id")
        if input_name == "crops.grow system type":
            return Vf_Grow.input_values("crops.grow system type")
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
        inputs = parse_inputs(Vf_Deliverables.input_type(), args)
        advanced_inputs = arr2dic(inputs["advanced"])
        output_type = Vf_Deliverables.output_type()
        result['sql_runtime'] = 0
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        def print_debug(model_name):
            print(model_name, result[model_name])

        # First, translate inputs into land status
        args_vf_input_land_status = dict()
        args_vf_input_land_status["land status"] = inputs["land status"]
        result["vf_input_land_status"] = Vf_Input_Land_Status.compute(
            args_vf_input_land_status, quantize_output=True)
        result['sql_runtime'] += result["vf_input_land_status"]['sql_runtime']

        print_debug('vf_input_land_status')
        # Next, compute sizes for models
        args_vf_sizing_area = dict()
        args_vf_sizing_area["area"] = inputs["site area"]
        args_vf_sizing_area["height"] = inputs["height"]
        args_vf_sizing_area["desired number of levels"] = inputs["number of levels"]
        args_vf_sizing_area["racking automation"] = False
        args_vf_sizing_area["processing"] = inputs["processing"]
        args_vf_sizing_area["cold storage"] = inputs["cold storage"]
        result["vf_sizing_area"] = Vf_Sizing_Area.compute(args_vf_sizing_area, quantize_output=True)
        result['sql_runtime'] += result["vf_sizing_area"]['sql_runtime']

        print_debug('vf_sizing_area')
        # Compute characteristics for grow system and crop
        args_vf_grow = dict()
        args_vf_grow["bedspace"] = result["vf_sizing_area"]["total bedspace"]
        args_vf_grow["crops"] = inputs["crops"]
        if "grow_system_depreciation_duration" in advanced_inputs:
            args_vf_grow["depreciation duration"] = advanced_inputs["grow_system_depreciation_duration"]
        result["vf_grow"] = Vf_Grow.compute(args_vf_grow, quantize_output=True)
        result['sql_runtime'] += result["vf_grow"]['sql_runtime']

        print_debug('vf_grow')
        set_if_unset(
            result,
            "bedspace",
            result["vf_sizing_area"]["total bedspace"])
        args_vf_crop_characteristics = dict()
        args_vf_crop_characteristics["crops"] = inputs["crops"]
        result["vf_crop_characteristics"] = Vf_Crop_Characteristics.compute(
            args_vf_crop_characteristics, quantize_output=True)
        result['sql_runtime'] += result["vf_crop_characteristics"]['sql_runtime']

        print_debug('vf_crop_characteristics')
        # Compute energy calculations
        args_vf_lighting = dict()
        args_vf_lighting["bedspace"] = result["vf_sizing_area"]["total bedspace"]
        args_vf_lighting["electricity cost"] = inputs["electricity cost"]
        # getting max optimal_ppfd among crops
        args_vf_lighting["ppfd"] = 0
        args_vf_lighting["lighting hours"] = 0
        for crop in result["vf_crop_characteristics"]['crops']:
            if args_vf_lighting["ppfd"] < crop['optimal avg ppfd']: args_vf_lighting["ppfd"] = crop['optimal avg ppfd']
            # determine lighting hours
            t = math.ceil(crop['optimal dli'] / crop['optimal avg ppfd'] * 1000000 / 3600)
            if t > args_vf_lighting["lighting hours"]: args_vf_lighting["lighting hours"] = t
        args_vf_lighting["lighting hours"] *= 365
        result["vf_lighting"] = Vf_Lighting.compute(args_vf_lighting, quantize_output=True)
        result['sql_runtime'] += result["vf_lighting"]['sql_runtime']

        print_debug('vf_lighting')
        args_vf_energy_setpoint_selection = dict()
        args_vf_energy_setpoint_selection["crops"] = inputs["crops"]
        result["vf_energy_setpoint_selection"] = Vf_Energy_Setpoint_Selection.compute(
            args_vf_energy_setpoint_selection, quantize_output=True)
        result['sql_runtime'] += result["vf_energy_setpoint_selection"]['sql_runtime']

        print_debug('vf_energy_setpoint_selection')
        args_vf_roof = dict()
        args_vf_roof["latitude"] = inputs["latitude"]
        args_vf_roof["longitude"] = inputs["longitude"]
        args_vf_roof["building insulation"] = inputs["building insulation"]
        args_vf_roof["building roof type"] = inputs["building roof type"]
        args_vf_roof["site area"] = inputs["site area"]
        args_vf_roof["height"] = inputs["height"]
        result["vf_roof"] = Vf_Roof.compute(args_vf_roof, quantize_output=True)
        result['sql_runtime'] += result["vf_roof"]['sql_runtime']

        print_debug('vf_roof')
        args_vf_energy = dict()
        args_vf_energy["latitude"] = inputs["latitude"]
        args_vf_energy["longitude"] = inputs["longitude"]
        args_vf_energy["site area"] = inputs["site area"]
        args_vf_energy["cover area"] = result["vf_roof"]["cover area"]
        args_vf_energy["U value"] = result["vf_roof"]["U value"]
        args_vf_energy["bedspace"] = result["vf_sizing_area"]["total bedspace"]
        args_vf_energy["photosynthetic efficacy"] = result["vf_lighting"]["photosynthetic efficacy"]
        args_vf_energy["crop optimal ppfd"] = result["vf_crop_characteristics"]["max optimal avg ppfd"]
        args_vf_energy["crop optimal dli"] = result["vf_crop_characteristics"]["avg optimal dli"]
        args_vf_energy["inside day temperature"] = result["vf_energy_setpoint_selection"]["inside day temperature"]
        args_vf_energy["inside night temperature"] = result["vf_energy_setpoint_selection"]["inside night temperature"]
        args_vf_energy["inside day relative humidity"] = result["vf_energy_setpoint_selection"]["inside day relative humidity"]
        args_vf_energy["inside night relative humidity"] = result["vf_energy_setpoint_selection"]["inside night relative humidity"]
        result["vf_energy"] = Vf_Energy.compute(args_vf_energy, quantize_output=True)
        result['sql_runtime'] += result["vf_energy"]['sql_runtime']

        print_debug('vf_energy')
        # Compute crop yield and revenue
        args_vf_crop = dict()
        args_vf_crop["bedspace"] = result["vf_sizing_area"]["total bedspace"]
        args_vf_crop["crops"] = result["vf_grow"]["crops"]
        args_vf_crop["provided light sum"] = result["vf_energy"]["provided light sum"]
        result["vf_crop"] = Vf_Crop.compute(args_vf_crop, quantize_output=True)
        result['sql_runtime'] += result["vf_crop"]['sql_runtime']

        print_debug('vf_crop')
        set_if_unset(
            result,
            "max total yield",
            result["vf_crop"]["total yield"])
        # Size up seeding area based on crop yield
        args_vf_sizing_seeding = dict()
        args_vf_sizing_seeding["annual number of plant sites"] = result["vf_crop"]["total plant sites"]
        args_vf_sizing_seeding["total substrate req"] = result["vf_crop"]["total substrate req"]
        result["vf_sizing_seeding"] = Vf_Sizing_Seeding.compute(
            args_vf_sizing_seeding, quantize_output=True)
        result['sql_runtime'] += result["vf_sizing_seeding"]['sql_runtime']

        print_debug('vf_sizing_seeding')
        # Compute wastage projection based on inputs and selected crops
        args_vf_wastage = dict()
        args_vf_wastage["grower experience"] = inputs["grower experience"]
        args_vf_wastage["organic production"] = inputs["organic production"]
        args_vf_wastage["crop base difficulty"] = result["vf_crop_characteristics"]["combined difficulty factor"]
        args_vf_wastage["number of crops"] = len(inputs["crops"])
        result["vf_wastage"] = Vf_Wastage.compute(args_vf_wastage, quantize_output=True)
        result['sql_runtime'] += result["vf_wastage"]['sql_runtime']

        print_debug('vf_wastage')
        # Compute capital expenditures
        args_vf_capex_overview = dict()
        args_vf_capex_overview["bedspace"] = result["vf_sizing_area"]["total bedspace"]
        args_vf_capex_overview["cold storage area fraction"] = result["vf_sizing_area"]["overall"][4]["percentage"]
        args_vf_capex_overview["bedspace seeding"] = result["vf_sizing_seeding"]["seeding bedspace needed non-microgreens"]
        args_vf_capex_overview["tons of cooling"] = result["vf_energy"]["tons of cooling"]
        args_vf_capex_overview["number of levels"] = result["vf_sizing_area"]["number of levels"]
        args_vf_capex_overview["grow system cost"] = result["vf_grow"]["cost"]
        args_vf_capex_overview["grow system depreciation"] = result["vf_grow"]["depreciation"]
        args_vf_capex_overview["real estate cost"] = inputs["site area"] * \
            inputs["land cost"] if result["vf_input_land_status"]["is buy"] else 0
        args_vf_capex_overview["building renovation cost"] = advanced_inputs['building_renovations_cost'] if 'building_renovations_cost' in advanced_inputs else result["vf_roof"]["cover cost"]
        args_vf_capex_overview["amount harvested per week"] = result["vf_crop"]["total yield"] / 52
        args_vf_capex_overview["electricity cost"] = inputs["electricity cost"]
        args_vf_capex_overview["ppfd"] = result["vf_crop_characteristics"]["max optimal avg ppfd"]
        args_vf_capex_overview['advanced'] = inputs['advanced']
        result["vf_capex_overview"] = Vf_Capex_Overview.compute(
            args_vf_capex_overview, quantize_output=True)
        result['sql_runtime'] += result["vf_capex_overview"]['sql_runtime']

        print_debug('vf_capex_overview')
        # Compute operating expenses
        args_vf_opex_overview = dict()
        args_vf_opex_overview["bedspace"] = result["vf_sizing_area"]["total bedspace"]
        args_vf_opex_overview["rent area"] = inputs["site area"] if result["vf_input_land_status"]["is rent"] else 0
        args_vf_opex_overview["annual number of plant sites"] = result["vf_sizing_seeding"]["seeding annual plug requirement"]
        args_vf_opex_overview["total area per week"] = result["vf_crop"]["total area per week"]
        args_vf_opex_overview["total substrate req"] = result["vf_crop"]["total substrate req"]
        args_vf_opex_overview["sale units"] = result["vf_crop"]["total sale units"]
        args_vf_opex_overview["seed cost per site"] = result["vf_crop_characteristics"]["combined seed price"]
        args_vf_opex_overview["microgreens seed cost per site"] = result["vf_crop_characteristics"]["combined substrate cost"]
        args_vf_opex_overview["lighting power"] = result["vf_energy"]["lighting power"]
        args_vf_opex_overview["hvac power"] = result["vf_energy"]["kBTU cooling"] / \
            result["vf_capex_overview"]["hvac seer"]
        args_vf_opex_overview["water transpired"] = result["vf_energy"]["water transpired"]
        args_vf_opex_overview["electricity cost"] = inputs["electricity cost"]
        args_vf_opex_overview["water cost"] = inputs["water cost"]
        args_vf_opex_overview["labor wages"] = inputs["labor wages"]
        args_vf_opex_overview["rent cost"] = 12 * inputs["rent cost"]
        args_vf_opex_overview["automation level"] = result["vf_capex_overview"]["automation level"]
        args_vf_opex_overview["owner is headgrower"] = inputs["owner is headgrower"]
        args_vf_opex_overview["headgrower salary"] = inputs["headGrowerSalary"]
        args_vf_opex_overview["packaging type"] = 1
        args_vf_opex_overview["revenue"] = result["vf_crop"]["total revenue"] * \
            (1 - result["vf_wastage"]["initial wastage"])
        args_vf_opex_overview["depreciation"] = result["vf_capex_overview"]["total depreciation"]
        args_vf_opex_overview["lighting hours per year"] = result["vf_energy"]["lighting time"]
        args_vf_opex_overview["advanced"] = inputs["advanced"]
        result["vf_opex_overview"] = Vf_Opex_Overview.compute(
            args_vf_opex_overview, quantize_output=True)
        result['sql_runtime'] += result["vf_opex_overview"]['sql_runtime']

        print_debug('vf_opex_overview')
        # Compute repayment schedule for debt
        # Financing amount is 0 if funding option is not debt
        args_financials_interest = dict()
        args_financials_interest["amount"] = result["vf_capex_overview"]["total cost"] if inputs["financing option"] == 1 else 0
        args_financials_interest["interest rate"] = inputs["interest rate"]
        args_financials_interest["repayment time"] = inputs["repayment time"]
        args_financials_interest["payments per year"] = 4
        result["financials_interest"] = Financials_Interest.compute(
            args_financials_interest, quantize_output=True)
        result['sql_runtime'] += result["financials_interest"]['sql_runtime']

        print_debug('financials_interest')
        # Combine outputs into financial model
        args_financials_main = dict()
        args_financials_main["capex line items"] = result["vf_capex_overview"]["line items"]
        args_financials_main["opex line items"] = result["vf_opex_overview"]["line items"]
        args_financials_main["wastage schedule"] = result["vf_wastage"]["wastage schedule"]
        args_financials_main["crops"] = result["vf_crop"]["crops"]
        args_financials_main["base yield"] = result["vf_crop"]["total yield"]
        args_financials_main["base revenue"] = result["vf_crop"]["total revenue"]
        args_financials_main["depreciation"] = result["vf_capex_overview"]["total depreciation"]
        args_financials_main["interest repayment schedule"] = result["financials_interest"]["repayment schedule"]
        args_financials_main["tax rate"] = inputs["tax rate"]
        result["financials_main"] = Financials_Main.compute(
            args_financials_main, quantize_output=True)
        result['sql_runtime'] += result["financials_main"]['sql_runtime']

        print_debug('financials_main')
        # Output number of employees
        args_financials_employees = dict()
        args_financials_employees["total staff"] = result["vf_opex_overview"]["total staff"]
        result["financials_employees"] = Financials_Employees.compute(
            args_financials_employees, quantize_output=True)
        result['sql_runtime'] += result["financials_employees"]['sql_runtime']

        print_debug('financials_employees')
        # Output population fed
        args_financials_population_fed = dict()
        args_financials_population_fed["total yield"] = result["vf_crop"]["total yield"]
        result["financials_population_fed"] = Financials_Population_Fed.compute(
            args_financials_population_fed, quantize_output=True)
        result['sql_runtime'] += result["financials_employees"]['sql_runtime']

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

        return result