from connection import fetch_one, fetch_all
from cachetools.func import ttl_cache
from caching import hashable_cache
from util import parse_inputs, preload_outputs, quantize_outputs, set_if_unset
import math


class Location_Rates:

    @staticmethod
    def info():
        return {
            "name": "location_rates",
            "description": "Converts a text address into latitude, longitude, and rate estimates"
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
            "gas cost": {
                "description": "Gas cost",
                "digits": 3,
                "type": "decimal"
            },
            "gas cost unit id": {
                "description": "Selected sale unit for gas cost",
                "digits": 0,
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
        inputs = parse_inputs(Location_Rates.input_type(), args)

        output_type = Location_Rates.output_type()
        preload_result = preload_outputs(output_type, inputs)
        for key in preload_result:
            result[key] = preload_result[key]

        from cached_geocoder import get_geocoder
        g = get_geocoder(inputs['location'])
        country = None
        state = None
        city = None
        latlng = None
        if g.json is not None:
            latlng = next(x.latlng for x in g if x.latlng)
            country_list = [x.country for x in g if x.country]
            country = country_list[0] if len(country_list) > 0 else None
            state_list = [x.state for x in g if x.state]
            state = state_list[0] if len(state_list) > 0 else None
            city_list = [x.city for x in g if x.city]
            city = city_list[0] if len(city_list) > 0 else None
        if g.json is None or country is None:
            g = get_geocoder(str(g.lat) + ', ' + str(g.lng))
            country_list = [x.country for x in g if x.country]
            country = country_list[0] if len(country_list) > 0 else None
            state_list = [x.state for x in g if x.state]
            state = state_list[0] if len(state_list) > 0 else None
            city_list = [x.city for x in g if x.city]
            city = city_list[0] if len(city_list) > 0 else None
        if latlng is not None:
            set_if_unset(result, 'latitude', latlng[0])
            set_if_unset(result, 'longitude', latlng[1])

        sql_city = "SELECT * FROM location_rates WHERE country=%(country)s AND state=%(state)s AND city=%(city)s"
        sql_state = "SELECT * FROM location_rates WHERE country=%(country)s AND state=%(state)s AND city IS NULL"
        sql_country = "SELECT * FROM location_rates WHERE country=%(country)s AND state IS NULL AND city IS NULL"
        sql_none = "SELECT * FROM location_rates WHERE country IS NULL AND state IS NULL AND city IS NULL"
        row_city = fetch_one(
            sql_city, {
                "country": country, "state": state, "city": city})
        row_state = fetch_one(sql_state, {"country": country, "state": state})
        row_country = fetch_one(sql_country, {"country": country})
        row_none = fetch_one(sql_none)
        column_to_key = {
            'electricity_cost': 'electricity cost',
            'farmworker_avg_wage': 'labor wages',
            'water_cost': 'water cost',
            'tax': 'tax rate',
            'rent_per_sqft': 'rent cost',
            'gas_cost': 'gas cost'
        }
        for row in [row_city, row_state, row_country, row_none]:
            if row is None:
                continue
            for column, value in row.items():
                if value is None or column not in column_to_key:
                    continue
                key = column_to_key[column]
                set_if_unset(result, key, value)

        set_if_unset(result, "gas cost unit id", 2)

        if quantize_output:
            quantize_outputs(output_type, result)

        return result
