from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math


class Soil_Cogp:

    @staticmethod
    def info():
        return {
            "name": "soil_cogp",
            "description": "Cost of Goods Produced for Soil"
        }

    @staticmethod
    def input_type():
        return {
            "plant sites": {
                "description": "Number of plant sites per year (plant site / y)",
                "type": "decimal"
            },
            "packaging type": {
                "type": "choice",
                "description": "Selection for packaging",
                "default": 2
            },
            "plug cost per site": {
                "description": "Cost per plug ($ / plant site)",
                "default": 0.0071,
                "type": "decimal"
            },
            "seed cost per site": {
                "description": "Cost per seed ($ / plant site)",
                "default": 0.005,
                "type": "decimal"
            },
            "fertilizer cost per site": {
                "description": "Cost per fertilizer ($ / plant site)",
                "default": 0.01,
                "type": "decimal"
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def input_values(input_name):
        if input_name == "packaging type":
            return fetch_all("SELECT id, description FROM packaging")
        raise ValueError("Unexpected input: " + str(input_name))

    @staticmethod
    def output_type():
        return {
            "total plug cost": {
                "description": "Total cost for plugs ($ / y)",
                "digits": 2,
                "type": "decimal"
            },
            "total seed cost": {
                "description": "Total cost for seeds ($ / y)",
                "digits": 2,
                "type": "decimal"
            },
            "total fertilizer cost": {
                "description": "Total cost for fertilizer ($ / y)",
                "digits": 2,
                "type": "decimal"
            },
            "total packaging cost": {
                "description": "Total cost per packaging ($ / y)",
                "digits": 2,
                "type": "decimal"
            },
            "total cost per site": {
                "description": "Total cost per site ($ / y)",
                "digits": 2,
                "type": "decimal"
            },
            "total cost": {
                "description": "Total cost for COGS ($ / y)",
                "digits": 2,
                "type": "decimal"
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def compute(args, quantize_output=False):
        result = dict()
        inputs = parse_inputs(Soil_Cogp.input_type(), args)

        output_type = Soil_Cogp.output_type()
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        query_inputs = {"id": inputs["packaging type"]}
        packaging_row = fetch_one(
            "SELECT cost FROM packaging WHERE id=%(id)s",
            query_inputs)
        if packaging_row is None:
            raise ValueError(
                "No elements found for inputs: " +
                str(query_inputs))

        packaging_cost_per_site = packaging_row["cost"]
        set_if_unset(
            result,
            "total plug cost",
            inputs["plug cost per site"] *
            inputs["plant sites"])
        set_if_unset(
            result,
            "total seed cost",
            inputs["seed cost per site"] *
            inputs["plant sites"])
        set_if_unset(
            result,
            "total fertilizer cost",
            inputs["fertilizer cost per site"] *
            inputs["plant sites"])
        set_if_unset(
            result,
            "total packaging cost",
            packaging_cost_per_site *
            inputs["plant sites"])
        set_if_unset(
            result,
            "total cost per site",
            inputs["plug cost per site"] +
            inputs["seed cost per site"] +
            inputs["fertilizer cost per site"] +
            packaging_cost_per_site)
        set_if_unset(
            result,
            "total cost",
            result["total plug cost"] +
            result["total seed cost"] +
            result["total fertilizer cost"] +
            result["total packaging cost"])

        if quantize_output:
            quantize_outputs(output_type, result)

        return result
