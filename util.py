from mdecimal import MDecimal


def parse_inputs(input_type, args):
    """
    Returns a new dictionary with parsed input types.
    If the input type is not present on args, use a default value if available, or fail otherwise
    :param input_type: Description of the input types
    :param args: Arguments to parse
    :return: A new dictionary with parsed input types
    """
    result = dict()
    for key in input_type:
        input_type_value = input_type[key]
        input_type_value_type = input_type_value["type"]
        if key in args:
            arg_value = args[key]
        elif "default" in input_type_value:
            arg_value = input_type_value["default"]
        else:
            raise ValueError("Missing input with no default value: " + key)

        
        if input_type_value_type == "choice":
            value = int(arg_value)
        elif input_type_value_type == "integer":
            value = int(arg_value)
        elif input_type_value_type == "decimal":
            value = MDecimal(arg_value)
        elif input_type_value_type == "list":
            value = []
            input_type_value_element_type = input_type_value["element type"]
            for list_element in arg_value:
                value.append(parse_inputs(input_type_value_element_type, list_element))
        else:
            value = arg_value

        result[key] = value

    return result


def preload_outputs(output_type, args):
    """
    Utility method to preload overwritten output arguments
    :param output_type: Specification of types of values to be loaded
    :param args: Dictionary to load values from
    :return: A new dictionary with the preloaded keys
    """
    result = dict()
    for key in args:
        if key in output_type:
            key_type = output_type[key]["type"]
            if key_type == "integer":
                result[key] = int(args[key])
            elif key_type == "decimal":
                result[key] = float(args[key])
            elif key_type == "list":
                arg_list = []
                list_element_type = output_type[key]["element type"]
                for list_element in args[key]:
                    arg_list.append(preload_outputs(list_element_type, list_element))
                result[key] = arg_list
            else:
                result[key] = args[key]
        else:
            result[key] = args[key]
    return result


def set_if_unset(result, key, value):
    """
    Utility method to set a new key within a dictionary if not yet set
    :param result: Dictionary to be modified
    :param key: Dictionary key to potentially be set
    :param value: New value to be set
    :return:
    """
    if key in result:
        return
    result[key] = value


def quantize_outputs(output_type, result):
    """
    Utility method to quantize decimal output types within a result dict
    :param output_type: Description of output keys
    :param result: dictionary with values to be quantized
    """
    to_delete = []
    for key in result:
        # Ensure only keys on output type are passed through
        if key not in output_type:
            to_delete.append(key)
            continue
        output_type_key = output_type[key]
        key_type = output_type_key["type"]
        if key_type == "decimal" and "digits" in output_type_key:
            digits = int(output_type_key["digits"])
            if result[key] is not None:
                result[key] = MDecimal(result[key]).quantize(MDecimal(10) ** -digits)
        elif key_type == "integer":
            result[key] = int(result[key])
        elif key_type == "list":
            output_type_key_element_type = output_type_key["element type"]
            for list_element in result[key]:
                quantize_outputs(output_type_key_element_type, list_element)

    for key in to_delete:
        del result[key]


def arr2dic(arr):
    """
    Utility method to conver array of objects into a dictionary
    :param arr: array of objects {description, value}
    :return: dict{description: value}
    """
    result = {}
    for item in arr:
        result[item["description"]] = item["value"]
    return result
