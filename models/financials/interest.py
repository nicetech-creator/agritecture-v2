from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math
import timeit

class Financials_Interest:

    @staticmethod
    def info():
        return {
            "name": "financials_interest",
            "description": "Interest payment calculations"
        }

    @staticmethod
    def input_type():
        return {
            "amount": {
                "description": "Initial value of loan ($)",
                "type": "decimal"
            },
            "interest rate": {
                "description": "Interest rate (%)",
                "default": 0.08,
                "type": "decimal"
            },
            "repayment time": {
                "description": "Repayment time (years)",
                "default": 7,
                "type": "decimal"
            },
            "payments per year": {
                "description": "Number of payments per year",
                "default": 4,
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
            "repayment amount": {
                "description": "Amount for each repayment ($)",
                "digits": 2,
                "type": "decimal"
            },
            "repayment schedule": {
                "type": "list",
                "description": "Repayment amounts per year",
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
        inputs = parse_inputs(Financials_Interest.input_type(), args)

        output_type = Financials_Interest.output_type()
        result['sql_runtime'] = 0
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        # Interest rate per payment period
        j = (1 + inputs["interest rate"]) ** (1 /
                                              inputs["payments per year"]) - 1
        # Number of payments
        n = inputs["payments per year"] * inputs["repayment time"]
        # Initial debt
        d = inputs["amount"]
        # Payment amount per period
        if j != 0:
            p = j * d * (j + 1)**(n - 1) / ((j + 1)**n - 1)
        else:
            p = d / n
        # Round cents up
        p = math.ceil(100 * p) / 100
        set_if_unset(result, "repayment amount", p)
        num_full_years = int(math.floor(inputs["repayment time"]))
        full_year_repayment_amount = p * inputs["payments per year"]
        partial_year_repayment_amount = full_year_repayment_amount * \
            (inputs["repayment time"] - num_full_years)
        repayment_schedule = []
        if d != 0:
            for year in range(1, num_full_years + 1):
                repayment_schedule.append({
                    'year': year,
                    'amount': full_year_repayment_amount
                })
            if partial_year_repayment_amount > 0:
                repayment_schedule.append({
                    'year': num_full_years + 1,
                    'amount': partial_year_repayment_amount
                })

        set_if_unset(result, "repayment schedule", repayment_schedule)
        result['runtime'] = timeit.default_timer() - start
        if quantize_output:
            quantize_outputs(output_type, result)

        return result