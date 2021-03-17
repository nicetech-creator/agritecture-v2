from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math

from models.location.rates import Location_Rates


class Vf_Defaults:

    @staticmethod
    def info():
        return {
            "name": "vf_defaults",
            "description": "Default input values for VF"
        }

    @staticmethod
    def input_type():
        return {
            "location": {
                "type": "text",
                "description": "Text description of the location"
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def input_values(input_name):
        raise ValueError("Unexpected input: " + str(input_name))

    @staticmethod
    def output_type():
        return {
            "latitude": {
                "description": "Geographical latitude",
                "digits": 7,
                "type": "decimal"
            },
            "longitude": {
                "description": "Geographical longitude",
                "digits": 7,
                "type": "decimal"
            },
            "electricity cost": {
                "description": "Cost of electricity ($ / kWh)",
                "digits": 4,
                "type": "decimal"
            },
            "labor wages": {
                "description": "Labor wages ($ / h)",
                "digits": 4,
                "type": "decimal"
            },
            "water cost": {
                "description": "Water cost ($ / gallon)",
                "digits": 7,
                "type": "decimal"
            },
            "tax rate": {
                "description": "Tax rate (%)",
                "digits": 4,
                "type": "decimal"
            },
            "rent cost": {
                "description": "Rent cost per area ($ / sqft month)",
                "digits": 4,
                "type": "decimal"
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def compute(args, quantize_output=False):
        result = dict()
        inputs = parse_inputs(Vf_Defaults.input_type(), args)

        output_type = Vf_Defaults.output_type()
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        args_location_rates = dict()
        args_location_rates["location"] = inputs["location"]
        result["location_rates"] = Location_Rates.compute(args_location_rates, quantize_output=quantize_output)

        set_if_unset(result, "latitude", result["location_rates"]["latitude"])
        set_if_unset(
            result,
            "longitude",
            result["location_rates"]["longitude"])
        set_if_unset(
            result,
            "electricity cost",
            result["location_rates"]["electricity cost"])
        set_if_unset(
            result,
            "labor wages",
            result["location_rates"]["labor wages"])
        set_if_unset(
            result,
            "water cost",
            result["location_rates"]["water cost"])
        set_if_unset(result, "tax rate", result["location_rates"]["tax rate"])
        set_if_unset(
            result,
            "rent cost",
            result["location_rates"]["rent cost"])

        if quantize_output:
            quantize_outputs(output_type, result)

        return result
