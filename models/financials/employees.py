from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math
import timeit

class Financials_Employees:

    @staticmethod
    def info():
        return {
            "name": "financials_employees",
            "description": "Employee number estimation"
        }

    @staticmethod
    def input_type():
        return {
            "total staff": {
                "type": "decimal",
                "description": "Total staff on labor requirements"
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def input_values(input_name):
        raise ValueError("Unexpected input: " + str(input_name))

    @staticmethod
    def output_type():
        return {
            "number of people employed": {
                "description": "Estimated number of people employed",
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
        inputs = parse_inputs(Financials_Employees.input_type(), args)

        output_type = Financials_Employees.output_type()
        result['sql_runtime'] = 0
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        num_employees = max(2, int(inputs['total staff']))
        num_employees_lower = int(
            min(num_employees - 1, math.floor(num_employees * 0.9)))
        num_employees_higher = int(
            max(num_employees + 1, math.ceil(num_employees * 1.1)))
        num_employees_str = str(num_employees_lower) + \
            ' - ' + str(num_employees_higher)

        set_if_unset(result, "number of people employed", num_employees_str)
        result['runtime'] = timeit.default_timer() - start
        if quantize_output:
            quantize_outputs(output_type, result)

        return result