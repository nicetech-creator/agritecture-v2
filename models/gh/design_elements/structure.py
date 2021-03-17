from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math
import timeit

class Gh_Structure_Design_Elements:

    @staticmethod
    def info():
        return {
            "name": "gh_structure_design_elements",
            "description": "Structure selector design elements in GH models"
        }

    @staticmethod
    def input_type():
        return {
            "element_id": {
                "type": "choice",
                "description": "Selection for structure",
                "default": 1
            },
            "bays area": {
                "description": "Area occupied by bays (sqft)",
                "type": "decimal"
            },
            "span width": {
                "description": "Span width (ft)",
                "default": 50,
                "type": "decimal"
            },
            "span length": {
                "description": "Span length (ft)",
                "default": 150,
                "type": "decimal"
            },
            "gutter height": {
                "description": "Gutter height (ft)",
                "default": 9.8,
                "type": "decimal"
            },
            "max height": {
                "description": "Max height (ft)",
                "default": 14.8,
                "type": "decimal"
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def input_values(input_name):
        if input_name == "element_id":
            return fetch_all(
                "SELECT id, element FROM gh_design_elements_structure")
        raise ValueError("Unexpected input: " + str(input_name))

    @staticmethod
    def output_type():
        return {
            "description": {
                "description": "Element description",
                "type": "text"
            },
            "investment per area": {
                "description": "Required investment per area ($ / sqft)",
                "digits": 2,
                "type": "decimal"
            },
            "operating cost": {
                "description": "Estimated operating cost ($ / sqft / year)",
                "digits": 2,
                "type": "decimal"
            },
            "number of spans": {
                "description": "Number of spans",
                "digits": 1,
                "type": "decimal"
            },
            "roof perimeter": {
                "description": "Greenhouse root perimeter (ft)",
                "digits": 1,
                "type": "decimal"
            },
            "footprint": {
                "description": "Greenhouse footprint (sqft)",
                "digits": 1,
                "type": "decimal"
            },
            "cover area": {
                "description": "Cover area (sqft)",
                "digits": 1,
                "type": "decimal"
            },
            "volume": {
                "description": "Total greenhouse volume (cuft)",
                "digits": 1,
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
            "element_id": {
                "type": "choice",
                "description": "Selection for structure",
                "default": 1
            }
        }

    @staticmethod
    @hashable_cache(ttl_cache())
    def compute(args, quantize_output=False):
        start = timeit.default_timer()
        result = dict()
        inputs = parse_inputs(Gh_Structure_Design_Elements.input_type(), args)

        output_type = Gh_Structure_Design_Elements.output_type()
        result['sql_runtime'] = 0
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        query_inputs = {"element_id": inputs["element_id"]}
        s_start = timeit.default_timer()
        structure_row = fetch_one(
            "SELECT description, investment_per_area, (maintenance + linear_depreciation) * investment_per_area operating_cost FROM gh_design_elements_structure WHERE id=%(element_id)s",
            query_inputs)
        result['sql_runtime'] += timeit.default_timer() - s_start
        if structure_row is None:
            raise ValueError(
                "No elements found for inputs: " +
                str(query_inputs))

        set_if_unset(result, "description", structure_row["description"])
        set_if_unset(
            result,
            "investment per area",
            structure_row["investment_per_area"])
        set_if_unset(result, "operating cost", structure_row["operating_cost"])
        set_if_unset(result, "number of spans",
                     inputs["bays area"] / (inputs["span width"] * inputs["span length"]))
        set_if_unset(result, "footprint", inputs["bays area"])
        l = inputs["span length"]
        w = inputs["span width"]
        gh = inputs["gutter height"]
        dh = inputs["max height"] - inputs["gutter height"]
        n = result["number of spans"]
        set_if_unset(result, "roof perimeter", (math.pi/2) *
                     (3*(w/2 + dh) - math.sqrt((3*w/2 + dh) * (w/2 + 3*dh))))
        rp = result["roof perimeter"]
        fp = result["footprint"]
        set_if_unset(result, "cover area", 2 * w * gh * n + 2 *
                     l * gh + rp * n * l + math.pi * dh * (w/2) * n)
        set_if_unset(result, "volume", fp * gh + n * l * (math.pi * w * dh)/4)
        result['runtime'] = timeit.default_timer() - start
        if quantize_output:
            quantize_outputs(output_type, result)

        return result
