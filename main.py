import simplejson as json
import importlib
from collections import namedtuple

model_to_class = {}
Module = namedtuple('Module', 'module class_name')

model_lazy_map = {
    'crop_grow_system': Module('models.util.crop_grow_system', 'Crop_Grow_System'),
    'location_query': Module('models.location.location_query', 'Location_Query'),
    'climate_query': Module('models.location.climate_query', 'Climate_Query'),
    'location_rates': Module('models.location.rates', 'Location_Rates'),
    'financials_interest': Module('models.financials.interest', 'Financials_Interest'),
    'financials_main': Module('models.financials.main', 'Financials_Main'),
    'financials_employees': Module('models.financials.employees', 'Financials_Employees'),
    'financials_population_fed': Module('models.financials.population_fed', 'Financials_Population_Fed'),
    'gh_input_land_status': Module('models.gh.input.land_status', 'Gh_Input_Land_Status'),
    'gh_input_structure': Module('models.gh.input.structure', 'Gh_Input_Structure'),
    'gh_wastage': Module('models.gh.wastage.wastage', 'Gh_Wastage'),
    'gh_sizing_area': Module('models.gh.sizing.area', 'Gh_Sizing_Area'),
    'gh_sizing_seeding': Module('models.gh.sizing.seeding', 'Gh_Sizing_Seeding'),
    'gh_energy': Module('models.gh.energy.energy', 'Gh_Energy'),
    'gh_crop_characteristics': Module('models.gh.crop.characteristics', 'Gh_Crop_Characteristics'),
    'gh_crop': Module('models.gh.crop.crop', 'Gh_Crop'),
    'gh_covers_design_elements': Module('models.gh.design_elements.covers', 'Gh_Covers_Design_Elements'),
    'gh_structure_design_elements': Module('models.gh.design_elements.structure', 'Gh_Structure_Design_Elements'),
    'gh_cold_storage_capex': Module('models.gh.capex.cold_storage', 'Gh_Cold_Storage_Capex'),
    'gh_grow': Module('models.gh.capex.grow', 'Gh_Grow'),
    'gh_lighting': Module('models.gh.capex.lighting', 'Gh_Lighting'),
    'gh_heating': Module('models.gh.capex.heating', 'Gh_Heating'),
    'gh_cooling': Module('models.gh.capex.cooling', 'Gh_Cooling'),
    'gh_ventilation': Module('models.gh.capex.ventilation', 'Gh_Ventilation'),
    'gh_co2_injection': Module('models.gh.capex.co2_injection', 'Gh_Co2_Injection'),
    'gh_processing_capex': Module('models.gh.capex.processing', 'Gh_Processing_Capex'),
    'gh_propagation_capex': Module('models.gh.capex.propagation', 'Gh_Propagation_Capex'),
    'gh_seeding_germination_capex': Module('models.gh.capex.seeding_germination', 'Gh_Seeding_Germination_Capex'),
    'gh_capex_overview': Module('models.gh.capex.overview', 'Gh_Capex_Overview'),
    'gh_cogp': Module('models.gh.opex.cogp', 'Gh_Cogp'),
    'gh_heating_opex': Module('models.gh.opex.heating', 'Gh_Heating_Opex'),
    'gh_labor': Module('models.gh.opex.labor', 'Gh_Labor'),
    'gh_power': Module('models.gh.opex.power', 'Gh_Power'),
    'gh_rent': Module('models.gh.opex.rent', 'Gh_Rent'),
    'gh_water': Module('models.gh.opex.water', 'Gh_Water'),
    'gh_opex_overview': Module('models.gh.opex.overview', 'Gh_Opex_Overview'),
    'gh_deliverables': Module('models.gh.deliverables', 'Gh_Deliverables'),
    'gh_warning': Module('models.gh.warning', 'Gh_Warning'),
    'gh_defaults': Module('models.gh.defaults', 'Gh_Defaults'),
    'vf_input_land_status': Module('models.vf.input.land_status', 'Vf_Input_Land_Status'),
    'vf_sizing_area': Module('models.vf.sizing.area', 'Vf_Sizing_Area'),
    'vf_sizing_seeding': Module('models.vf.sizing.seeding', 'Vf_Sizing_Seeding'),
    'vf_wastage': Module('models.vf.wastage.wastage', 'Vf_Wastage'),
    'vf_roof': Module('models.vf.energy.roof', 'Vf_Roof'),
    'vf_energy_setpoint_selection': Module('models.vf.energy.setpoint_selection', 'Vf_Energy_Setpoint_Selection'),
    'vf_energy': Module('models.vf.energy.energy', 'Vf_Energy'),
    'vf_crop_characteristics': Module('models.vf.crop.characteristics', 'Vf_Crop_Characteristics'),
    'vf_crop': Module('models.vf.crop.crop', 'Vf_Crop'),
    'vf_cold_storage_capex': Module('models.vf.capex.cold_storage', 'Vf_Cold_Storage_Capex'),
    'vf_grow': Module('models.vf.capex.grow', 'Vf_Grow'),
    'vf_hvac': Module('models.vf.capex.hvac', 'Vf_Hvac'),
    'vf_racking': Module('models.vf.capex.racking', 'Vf_Racking'),
    'vf_lighting': Module('models.vf.capex.lighting', 'Vf_Lighting'),
    'vf_processing_capex': Module('models.vf.capex.processing', 'Vf_Processing_Capex'),
    'vf_propagation_capex': Module('models.vf.capex.propagation', 'Vf_Propagation_Capex'),
    'vf_seeding_germination_capex': Module('models.vf.capex.seeding_germination', 'Vf_Seeding_Germination_Capex'),
    'vf_capex_overview': Module('models.vf.capex.overview', 'Vf_Capex_Overview'),
    'vf_labor': Module('models.vf.opex.labor', 'Vf_Labor'),
    'vf_rent': Module('models.vf.opex.rent', 'Vf_Rent'),
    'vf_cogp': Module('models.vf.opex.cogp', 'Vf_Cogp'),
    'vf_power': Module('models.vf.opex.power', 'Vf_Power'),
    'vf_water': Module('models.vf.opex.water', 'Vf_Water'),
    'vf_opex_overview': Module('models.vf.opex.overview', 'Vf_Opex_Overview'),
    'vf_deliverables': Module('models.vf.deliverables', 'Vf_Deliverables'),
    'vf_warning': Module('models.vf.warning', 'Vf_Warning'),
    'vf_defaults': Module('models.vf.defaults', 'Vf_Defaults'),
    'soil_input_land_status': Module('models.soil.input.land_status', 'Soil_Input_Land_Status'),
    'soil_sizing_area': Module('models.soil.sizing.area', 'Soil_Sizing_Area'),
    'soil_crop_characteristics': Module('models.soil.crop.characteristics', 'Soil_Crop_Characteristics'),
    'soil_crop': Module('models.soil.crop.crop', 'Soil_Crop'),
    'soil_crop_cycles': Module('models.soil.crop.cycles', 'Soil_Crop_Cycles'),
    'soil_capex_overview': Module('models.soil.capex.overview', 'Soil_Capex_Overview'),
    'soil_rent': Module('models.soil.opex.rent', 'Soil_Rent'),
    'soil_water': Module('models.soil.opex.water', 'Soil_Water'),
    'soil_labor': Module('models.soil.opex.labor', 'Soil_Labor'),
    'soil_cogp': Module('models.soil.opex.cogp', 'Soil_Cogp'),
    'soil_energy': Module('models.soil.opex.energy', 'Soil_Energy'),
    'soil_opex_overview': Module('models.soil.opex.overview', 'Soil_Opex_Overview'),
    'soil_wastage': Module('models.soil.wastage.wastage', 'Soil_Wastage'),
    'soil_defaults': Module('models.soil.defaults', 'Soil_Defaults'),
    'soil_deliverables': Module('models.soil.deliverables', 'Soil_Deliverables'),
    'soil_warning': Module('models.soil.warning', 'Soil_Warning'),
    'price_market_research': Module('models.pricing.price_market_research', 'Price_Market_Research'),
    'ad_setting': Module('models.setting.main', 'AD_Setting'),
    'orm_crop': Module('models.orm.crop', 'ORM_Crop'),
}


def __get_model_class(model_name):
    global model_to_class

    if model_name in model_to_class:
        return model_to_class[model_name]

    if model_name not in model_lazy_map:
        raise ValueError('Model name not found: ' + model_name)

    module = model_lazy_map[model_name]
    model_class = getattr(
        importlib.import_module(
            module.module),
        module.class_name)
    model_to_class[model_name] = model_class

    return model_class


def __read_param(request_json, request_args, param_name):
    """
    Convenience method to parse param_name out of the provided request
    :param request_json:
    :param request_args:
    :param param_name:
    :return:
    """
    if request_json and param_name in request_json:
        return request_json[param_name]
    if request_args and param_name in request_args:
        return request_args[param_name]

    return None


def cors_decorator(func):
    def func_wrapper(request):
        # Set CORS headers for the preflight request
        if request.method == 'OPTIONS':
            # Allows GET requests from any origin with the Content-Type
            # header and caches preflight response for an 3600s
            headers = {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Max-Age': '3600'
            }

            return ('', 204, headers)

        # Set CORS headers for the main request
        headers = {
            'Access-Control-Allow-Origin': '*'
        }

        return (func(request), 200, headers)

    return func_wrapper


ACCESS_CONTROL_ALLOW_ORIGIN = 'https://mydomain.com'


def cors_decorator_auth(func):
    def func_wrapper(request):
        # Set CORS headers for preflight requests
        if request.method == 'OPTIONS':
            # Allows GET requests from origin domain with Authorization header
            headers = {
                'Access-Control-Allow-Origin': ACCESS_CONTROL_ALLOW_ORIGIN,
                'Access-Control-Allow-Methods': 'GET',
                'Access-Control-Allow-Headers': 'Authorization',
                'Access-Control-Max-Age': '3600',
                'Access-Control-Allow-Credentials': 'true'
            }
            return ('', 204, headers)

        # Set CORS headers for the main request
        headers = {
            'Access-Control-Allow-Origin': ACCESS_CONTROL_ALLOW_ORIGIN,
            'Access-Control-Allow-Credentials': 'true'
        }

        return (func(request), 200, headers)

    return func_wrapper

@cors_decorator
def query_model(request):
    request_json = request.get_json(silent=True)
    request_args = request.args

    print('Function call',
          {'request_json': request_json,
           'request_args': request_args})

    model = __read_param(request_json, request_args, "model")

    if model is None:
        raise ValueError("Unspecified parameter: model")
    else:
        model_class = __get_model_class(model)

    cmd = __read_param(request_json, request_args, "cmd")

    if cmd == "info":
        result = json.dumps(model_class.info())
        print({'result': result})
        return result

    if cmd == "inputs":
        return json.dumps(model_class.input_type())

    if cmd == "input_values":
        input = __read_param(request_json, request_args, "input")
        if input is None:
            raise ValueError("Unspecified cmd parameter: input")
        result = json.dumps(model_class.input_values(input))
        print({'result': result})
        return result

    if cmd == "outputs":
        result = json.dumps(model_class.output_type())
        print({'result': result})
        return result

    if cmd == "compute":
        args = __read_param(request_json, request_args, "args")
        if args is None:
            raise ValueError("Unspecified cmd parameter: args")
        result = json.dumps(model_class.compute(args, quantize_output=True))
        print({'result': result})
        return result

    if cmd == "debug":
        args = __read_param(request_json, request_args, "args")
        if args is None:
            raise ValueError("Unspecified cmd parameter: args")
        result = json.dumps(model_class.compute(args, quantize_output=False))
        print({'result': result})
        return result

    if cmd is not None:
        raise ValueError("Unknown command: " + cmd)
    else:
        raise ValueError("Unspecified parameter: cmd")
