from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math
import timeit

class Vf_Processing_Capex:

    @staticmethod
    def info():
        return {
            "name": "vf_processing_capex",
            "description": "Processing system selector for VF Capex"
        }

    @staticmethod
    def input_type():
        return {
            "amount harvested per week": {
                "description": "Amount harvested per week (lb / w)",
                "type": "decimal"
            },
            "depreciation duration": {
                "description": "Duration of item depreciation",
                "default": 20,
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
            "harvester description": {
                "description": "Description of selected harvester",
                "type": "text"
            },
            "washer description": {
                "description": "Description of selected washer",
                "type": "text"
            },
            "number of harvesters": {
                "description": "Number of harvesters required to cover bedspace",
                "type": "integer"
            },
            "number of washers": {
                "description": "Number of washers required to cover bedspace",
                "type": "integer"
            },
            "harvester cost": {
                "description": "Total cost for harvesters ($)",
                "digits": 2,
                "type": "decimal"
            },
            "washer cost": {
                "description": "Total cost for washers ($)",
                "digits": 2,
                "type": "decimal"
            },
            "harvester automation level": {
                "description": "Automation level for harvester",
                "type": "text"
            },
            "washer automation level": {
                "description": "Automation level for washer",
                "type": "text"
            },
            "processing tables cost": {
                "description": "Total cost for processing tables ($)",
                "digits": 2,
                "type": "decimal"
            },
            "cost": {
                "description": "Total cost for processing ($)",
                "digits": 2,
                "type": "decimal"
            },
            "depreciation": {
                "description": "Depreciation cost per year ($ / y)",
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
            },
            "depreciation duration": {
                "description": "Duration of item depreciation",
                "default": 20,
                "type": "decimal"
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def compute(args, quantize_output=False):
        start = timeit.default_timer()
        result = dict()
        inputs = parse_inputs(Vf_Processing_Capex.input_type(), args)

        output_type = Vf_Processing_Capex.output_type()
        result['sql_runtime'] = 0
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        query_inputs = {"amount": inputs["amount harvested per week"]}
        s_start = timeit.default_timer()
        harvester_row = fetch_one(
            "select description, price, pounds_per_week, automation from (select *, lead(pounds_per_week) over (order by pounds_per_week) next_pounds_per_week from harvester_lookup) k where pounds_per_week >= %(amount)s or next_pounds_per_week is null limit 1",
            query_inputs)
        if harvester_row is None:
            raise ValueError(
                "No elements found for inputs: " +
                str(query_inputs))

        query_inputs = {"amount": inputs["amount harvested per week"]}
        washer_row = fetch_one(
            "select description, price, pounds_per_week, automation from (select *, lead(pounds_per_week) over (order by pounds_per_week) next_pounds_per_week from washer_lookup) k where pounds_per_week >= %(amount)s or next_pounds_per_week is null limit 1",
            query_inputs)
        result['sql_runtime'] += timeit.default_timer() - s_start
        if washer_row is None:
            raise ValueError(
                "No elements found for inputs: " +
                str(query_inputs))

        set_if_unset(
            result,
            "harvester description",
            harvester_row["description"])
        set_if_unset(
            result,
            "number of harvesters",
            0 if harvester_row["price"] == 0 else math.ceil(
                inputs["amount harvested per week"] /
                harvester_row["pounds_per_week"]))
        set_if_unset(
            result,
            "harvester cost",
            harvester_row["price"] *
            result["number of harvesters"])
        set_if_unset(
            result,
            "harvester automation level",
            harvester_row["automation"])
        set_if_unset(result, "washer description", washer_row["description"])
        set_if_unset(
            result,
            "number of washers",
            0 if washer_row["price"] == 0 else math.ceil(
                inputs["amount harvested per week"] /
                washer_row["pounds_per_week"]))
        set_if_unset(
            result,
            "washer cost",
            washer_row["price"] *
            result["number of washers"])
        set_if_unset(
            result,
            "washer automation level",
            washer_row["automation"])
        set_if_unset(result, "processing tables cost", 2500)
        set_if_unset(
            result,
            "cost",
            result["harvester cost"] +
            result["washer cost"] +
            result["processing tables cost"])
        set_if_unset(result, "depreciation", result["cost"] / inputs['depreciation duration'])

        result['runtime'] = timeit.default_timer() - start
        if quantize_output:
            quantize_outputs(output_type, result)

        return result