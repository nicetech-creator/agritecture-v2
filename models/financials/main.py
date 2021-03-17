from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math
import timeit

class Financials_Main:

    @staticmethod
    def info():
        return {
            "name": "financials_main",
            "description": "Main financials model"
        }

    @staticmethod
    def input_type():
        return {
            "capex line items": {
                "type": "list",
                "description": "Line item costs for Capex",
                "element type": {
                    "description": {
                        "type": "text",
                        "description": "Line item description"
                    },
                    "cost": {
                        "description": "Line item expenses cost ($)",
                        "digits": 2,
                        "type": "decimal"
                    }
                }
            },
            "opex line items": {
                "type": "list",
                "description": "Line item costs for Opex",
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
            "wastage schedule": {
                "type": "list",
                "description": "Projected wastage for each year",
                "element type": {
                    "year": {
                        "type": "integer",
                        "description": "Year for projection"
                    },
                    "wastage": {
                        "digits": 4,
                        "description": "Projected wastage for this year (%)",
                        "type": "decimal"
                    }
                }
            },
            "crops": {
                "type": "list",
                "description": "Crop matrix results",
                "element type": {
                    "crop type": {
                        "type": "text",
                        "description": "Name of the crop"
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
                    "price per unit": {
                        "digits": 2,
                        "description": "Price obtained per unit ($ / unit)",
                        "type": "decimal"
                    },
                    "system fraction": {
                        "description": "Fraction of bedspace allocated to crop",
                        "type": "decimal"
                    },
                    "sale unit id": {
                        "type": "choice",
                        "description": "Selected sale unit for crop item"
                    },
                }
            },
            "base yield": {
                "description": "Base projected yield per year, before wastage (lb / y)",
                "type": "decimal"
            },
            "base revenue": {
                "description": "Base projected revenue per year, before wastage ($ / y)",
                "type": "decimal"
            },
            "depreciation": {
                "description": "Yearly amount of depreciation ($ / y)",
                "type": "decimal"
            },
            "interest repayment schedule": {
                "type": "list",
                "description": "Interest repayment amounts per year",
                "element type": {
                    "year": {
                        "type": "integer",
                        "description": "Year"
                    },
                    "amount": {
                        "description": "Repayment amount for this year ($)",
                        "digits": 2,
                        "type": "decimal"
                    }
                }
            },
            "tax rate": {
                "description": "Tax rate (%)",
                "type": "decimal"
            },
            "discount rate": {
                "description": "Discount rate for PV of cash flows (%)",
                "default": 0.08,
                "type": "decimal"
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def input_values(input_name):
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
                                "type": "decimal",
                                "description": "The price on which seller can make net profit 0 on the market."
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
        inputs = parse_inputs(Financials_Main.input_type(), args)

        output_type = Financials_Main.output_type()
        result['sql_runtime'] = 0
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        # Summarize capex
        result['capex breakout'] = []
        for line in inputs['capex line items']:
            result['capex breakout'].append({
                'category': line['description'],
                'total price': line['cost']
            })
        total_capex = sum(line['cost'] for line in inputs['capex line items'])
        for line in result['capex breakout']:
            line['fraction'] = line['total price'] / total_capex
        result['capex breakout'].append({
            'category': 'Total',
            'total price': total_capex,
            'fraction': 1
        })
        # Summarize cogs & opex
        result['cogs & opex breakout'] = []
        for line in inputs['opex line items']:
            result['cogs & opex breakout'].append({
                'category': line['description'],
                'total price': line['cost']
            })
        total_cogs_opex = sum(line['cost']
                              for line in inputs['opex line items'])
        for line in result['cogs & opex breakout']:
            line['fraction'] = line['total price'] / total_cogs_opex
        result['cogs & opex breakout'].append({
            'category': 'Total',
            'total price': total_cogs_opex,
            'fraction': 1
        })
        # Summarize cogs
        result['cogs breakout'] = []
        for line in inputs['opex line items']:
            result['cogs breakout'].append({
                'category': line['description'],
                'total price': line['cogs cost']
            })
        total_cogs = sum(line['cogs cost']
                         for line in inputs['opex line items'])
        result['cogs breakout'].append({
            'category': 'Total',
            'total price': total_cogs
        })
        # Summarize opex
        result['opex breakout'] = []
        for line in inputs['opex line items']:
            result['opex breakout'].append({
                'category': line['description'],
                'total price': line['opex cost']
            })
        total_opex = sum(line['opex cost']
                         for line in inputs['opex line items'])
        result['opex breakout'].append({
            'category': 'Total',
            'total price': total_opex
        })
        # Create annual summary breakdown

        def get_interest_payment(year):
            for row in inputs['interest repayment schedule']:
                if row['year'] == year:
                    return row['amount']
            return 0
        result['annual summary'] = []
        sale_unit_lookup = {r['id']: (r['description'], r['multiplier'], r['lb']) for r in fetch_all(
            "SELECT id, description, multiplier, lb FROM sale_units")}
        def get_lb(line):
            return sale_unit_lookup[line['sale unit id']][2]
        
        for idx, line in enumerate(inputs['wastage schedule']):
            year = line['year']
            wastage = line['wastage']
            # get base yield with lost factore applied
            base_yield = 0
            base_revenue = 0
            crop_yields = [] # yeild list for each crop
            for crop in inputs['crops']:
                single_crop_yield = crop['yield'] * crop['lost factors'][idx]['lost']
                single_crop_revenu = int(single_crop_yield * get_lb(crop)) * crop['price per unit']
                base_yield += single_crop_yield
                base_revenue += single_crop_revenu
                crop_yields.append({
                    "crop type": crop['crop type'],
                    "yield": single_crop_yield * (1 - wastage),
                    "revenue": single_crop_revenu * (1 - wastage),
                    "fraction": crop['system fraction'],
                })
            total_yield = base_yield * (1 - wastage)
            waste_adjusted_revenue = base_revenue * (1 - wastage)
            depreciation_amortization = inputs['depreciation']
            interest_payment = get_interest_payment(year)
            ebit = waste_adjusted_revenue - total_cogs - \
                total_opex - depreciation_amortization
            taxes = max(ebit - interest_payment, 0) * inputs['tax rate']
            net_profit = ebit - interest_payment - taxes
            ebitda = net_profit + depreciation_amortization + interest_payment + taxes

            # break even price calculation
            for crop in crop_yields:
                crop["even price"] = (crop["revenue"] - net_profit * crop['fraction']) / crop['yield']
            result['annual summary'].append({
                'year': year,
                'wastage': wastage,
                'total yield': total_yield,
                'crop yield': crop_yields,
                'waste-adjusted revenue': waste_adjusted_revenue,
                'cogs': total_cogs,
                'opex': total_opex,
                'ebitda': ebitda,
                'depreciation & amortization': depreciation_amortization,
                'interest payment': interest_payment,
                'taxes': taxes,
                'net profit': net_profit
            })
        # Create cash flow breakdown
        result['cash flow'] = []
        remaining_balance = total_capex
        for line in result['annual summary']:
            year = line['year']
            net_profit = line['net profit']
            d_a = line['depreciation & amortization']
            net_changes_in_working_capital = -0.05 * net_profit
            free_cash_flow = net_profit + d_a + net_changes_in_working_capital
            remaining_balance -= free_cash_flow
            if remaining_balance < 0:
                remaining_balance = 0
            result['cash flow'].append({
                'year': year,
                'net profit': net_profit,
                'd&a': d_a,
                'net changes in working capital': net_changes_in_working_capital,
                'free cash flow': free_cash_flow,
                'remaining balance': remaining_balance
            })
        # Compute PV of cash flows
        pv_of_cash_flows = 0
        discount_rate = inputs['discount rate']
        for line in result['cash flow']:
            year = int(line['year'])
            cash_flow = line['free cash flow']
            pv_of_cash_flows += cash_flow / ((1 + discount_rate) ** year)
        result['pv of cash flows'] = pv_of_cash_flows

        # Compute payback period
        sum_free_cash_flow = 0
        result['payback period'] = None
        for line in result['cash flow']:
            if (sum_free_cash_flow + line['free cash flow']) >= total_capex: # accumulated free cash flow is greater than total capex
                result['payback period'] = int(line['year']) + (total_capex - sum_free_cash_flow) / line['free cash flow'] - 1
                break
            sum_free_cash_flow += line['free cash flow']
        
        # Create financial summary
        result['financial summary'] = [{
            'capex': total_capex,
            'opex': total_opex,
            'cogs': total_cogs,
            'opex + cogs': total_cogs_opex,
            'max annual revenue': inputs['base revenue'],
            'payback period': result['payback period']
        }]
        # Create operating summary
        result['operating summary'] = []
        for year in [1, 5, 10]:
            year_summary = None
            for row in result['annual summary']:
                if row['year'] == year:
                    year_summary = row
                    break
            wastage = year_summary['wastage']
            waste_adjusted_revenue = year_summary['waste-adjusted revenue']
            ebitda = year_summary['ebitda']
            ebitda_margin = 0 if waste_adjusted_revenue == 0 else ebitda / waste_adjusted_revenue
            net_profit = year_summary['net profit']
            net_margin = 0 if waste_adjusted_revenue == 0 else net_profit / waste_adjusted_revenue
            result['operating summary'].append({
                'year': year,
                'wastage': wastage,
                'waste-adjusted revenue': waste_adjusted_revenue,
                'ebitda': ebitda,
                'ebitda margin': ebitda_margin,
                'net profit': net_profit,
                'net margin': net_margin
            })
        result['runtime'] = timeit.default_timer() - start
        if quantize_output:
            quantize_outputs(output_type, result)

        return result