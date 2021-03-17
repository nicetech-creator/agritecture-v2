from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math

from models.soil.input.land_status import Soil_Input_Land_Status
from models.soil.sizing.area import Soil_Sizing_Area
from models.soil.crop.characteristics import Soil_Crop_Characteristics
from models.soil.crop.crop import Soil_Crop
from models.soil.wastage.wastage import Soil_Wastage
from models.soil.opex.overview import Soil_Opex_Overview
from models.soil.capex.overview import Soil_Capex_Overview
from models.financials.interest import Financials_Interest
from models.financials.main import Financials_Main
from models.financials.employees import Financials_Employees
from models.financials.population_fed import Financials_Population_Fed


class Soil_Deliverables:

    @staticmethod
    def info():
        return {
            "name": "soil_deliverables",
            "description": "Deliverables for Soil"
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
            "organic production": {
                "description": "Whether the system will use organic production",
                "type": "bool",
                "default": False
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
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def input_values(input_name):
        if input_name == "land status":
            return Soil_Input_Land_Status.input_values("land status")
        if input_name == "grower experience":
            return Soil_Wastage.input_values("grower experience")
        if input_name == "crops.id":
            return fetch_all("SELECT id, crop_type FROM crops ORDER BY id")
        if input_name == "crops.sale unit id":
            return fetch_all(
                "SELECT id, description FROM sale_units ORDER BY id")
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
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def compute(args, quantize_output=False):
        result = dict()
        inputs = parse_inputs(Soil_Deliverables.input_type(), args)

        output_type = Soil_Deliverables.output_type()
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        def print_debug(model_name):
            print(model_name, result[model_name])

        # First, translate inputs into land status
        args_soil_input_land_status = dict()
        args_soil_input_land_status["land status"] = inputs["land status"]
        result["soil_input_land_status"] = Soil_Input_Land_Status.compute(
            args_soil_input_land_status)

        print_debug('soil_input_land_status')
        # Compute sizes for models
        args_soil_sizing_area = dict()
        args_soil_sizing_area["area"] = inputs["site area"]
        result["soil_sizing_area"] = Soil_Sizing_Area.compute(
            args_soil_sizing_area)

        print_debug('soil_sizing_area')
        # Compute crop characteristics
        args_soil_crop_characteristics = dict()
        args_soil_crop_characteristics["crops"] = inputs["crops"]
        result["soil_crop_characteristics"] = Soil_Crop_Characteristics.compute(
            args_soil_crop_characteristics)

        print_debug('soil_crop_characteristics')
        # Compute crop
        args_soil_crop = dict()
        args_soil_crop["latitude"] = inputs["latitude"]
        args_soil_crop["longitude"] = inputs["longitude"]
        args_soil_crop["bedspace"] = result["soil_sizing_area"]["main bedspace"]
        args_soil_crop["crops"] = inputs["crops"]
        result["soil_crop"] = Soil_Crop.compute(args_soil_crop)

        print_debug('soil_crop')
        set_if_unset(
            result,
            "max total yield",
            result["soil_crop"]["total yield"])
        set_if_unset(
            result,
            "bedspace",
            result["soil_sizing_area"]["main bedspace"])
        # Compute wastage projection based on inputs and selected crops
        args_soil_wastage = dict()
        args_soil_wastage["grower experience"] = inputs["grower experience"]
        args_soil_wastage["organic production"] = inputs["organic production"]
        args_soil_wastage["crop base difficulty"] = result["soil_crop_characteristics"]["combined difficulty factor"]
        args_soil_wastage["number of crops"] = len(inputs["crops"])
        result["soil_wastage"] = Soil_Wastage.compute(args_soil_wastage)

        print_debug('soil_wastage')
        # Compute capital expenditures
        args_soil_capex_overview = dict()
        args_soil_capex_overview["land area"] = inputs["site area"]
        args_soil_capex_overview["nursery trays"] = math.ceil(
            result["soil_crop"]["total plant sites"] / 128)
        args_soil_capex_overview["real estate cost"] = inputs["site area"] * \
            inputs["land cost"] if result["soil_input_land_status"]["is buy"] else 0
        args_soil_capex_overview["seeding equipment cost"] = 1500 if result["soil_crop"]["total plant sites"] > 4000 else 0
        result["soil_capex_overview"] = Soil_Capex_Overview.compute(
            args_soil_capex_overview)

        print_debug('soil_capex_overview')

        def get_capex_cost(description):
            for item in result['soil_capex_overview']['line items']:
                if item['description'].startswith(description):
                    return item['cost']
            return None

        # Compute operating expenses
        args_soil_opex_overview = dict()
        args_soil_opex_overview["bedspace"] = result["soil_sizing_area"]["main bedspace"]
        args_soil_opex_overview["rent area"] = inputs["site area"] if result["soil_input_land_status"]["is rent"] else 0
        args_soil_opex_overview["annual number of plant sites"] = result["soil_crop"]["total plant sites"]
        args_soil_opex_overview["water used"] = result["soil_crop"]["total water use"]
        args_soil_opex_overview["cold storage capex"] = get_capex_cost(
            'Cold Storage')
        args_soil_opex_overview["tools capex"] = get_capex_cost('Tools')
        args_soil_opex_overview["transportation capex"] = get_capex_cost(
            'Transportation')
        args_soil_opex_overview["harvest capex"] = get_capex_cost('Harvesting')
        args_soil_opex_overview["water cost"] = inputs["water cost"]
        args_soil_opex_overview["labor wages"] = inputs["labor wages"]
        args_soil_opex_overview["rent cost"] = 12 * inputs["rent cost"]
        args_soil_opex_overview["owner is headgrower"] = inputs["owner is headgrower"]
        args_soil_opex_overview["headgrower salary"] = inputs["headGrowerSalary"]
        args_soil_opex_overview["packaging type"] = 1
        args_soil_opex_overview["revenue"] = result["soil_crop"]["total revenue"] * (
            1 - result["soil_wastage"]["initial wastage"])
        args_soil_opex_overview["depreciation"] = result["soil_capex_overview"]["total depreciation"]
        result["soil_opex_overview"] = Soil_Opex_Overview.compute(
            args_soil_opex_overview)

        print_debug('soil_opex_overview')
        # Compute repayment schedule for debt
        # Financing amount is 0 if funding option is not debt
        args_financials_interest = dict()
        args_financials_interest["amount"] = result["soil_capex_overview"]["total cost"] if inputs["financing option"] == 1 else 0
        args_financials_interest["interest rate"] = inputs["interest rate"]
        args_financials_interest["repayment time"] = inputs["repayment time"]
        args_financials_interest["payments per year"] = 4
        result["financials_interest"] = Financials_Interest.compute(
            args_financials_interest)

        print_debug('financials_interest')
        # Combine outputs into financial model
        args_financials_main = dict()
        args_financials_main["capex line items"] = result["soil_capex_overview"]["line items"]
        args_financials_main["opex line items"] = result["soil_opex_overview"]["line items"]
        args_financials_main["wastage schedule"] = result["soil_wastage"]["wastage schedule"]
        args_financials_main["crops"] = result["soil_crop"]["crops"]
        args_financials_main["base yield"] = result["soil_crop"]["total yield"]
        args_financials_main["base revenue"] = result["soil_crop"]["total revenue"]
        args_financials_main["depreciation"] = result["soil_capex_overview"]["total depreciation"]
        args_financials_main["interest repayment schedule"] = result["financials_interest"]["repayment schedule"]
        args_financials_main["tax rate"] = inputs["tax rate"]
        result["financials_main"] = Financials_Main.compute(
            args_financials_main)

        print_debug('financials_main')
        # Output number of employees
        args_financials_employees = dict()
        args_financials_employees["total staff"] = result["soil_opex_overview"]["total staff"]
        result["financials_employees"] = Financials_Employees.compute(
            args_financials_employees)

        print_debug('financials_employees')
        # Output population fed
        args_financials_population_fed = dict()
        args_financials_population_fed["total yield"] = result["soil_crop"]["total yield"]
        result["financials_population_fed"] = Financials_Population_Fed.compute(
            args_financials_population_fed)

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

        if quantize_output:
            quantize_outputs(output_type, result)

        return result