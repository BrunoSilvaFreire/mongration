def deep_set(document, *args):
    # Check if the arguments are sufficient to perform the operation
    if len(args) < 2:
        raise ValueError("There must be at least two arguments after the document: keys and a value")

    # The last argument is the value to set
    value = args[-1]
    # All but the last argument are the keys
    keys = args[:-1]

    # Start from the document root
    current = document
    for key in keys[:-1]:  # Iterate over keys, stopping before the last one
        # If the key doesn't exist or is not a dictionary, create/overwrite it with an empty dictionary
        if key not in current or not isinstance(current[key], dict):
            current[key] = {}
        # Move deeper into the document
        current = current[key]

    # Set the value at the final key
    current[keys[-1]] = value


def deep_drop(document, *args):
    if len(args) < 1:
        raise ValueError("At least one key must be provided")

    # Navigate to the deepest dictionary before the target key
    current = document
    for key in args[:-1]:  # Stop before the last key, which is the one to drop
        if key in current and isinstance(current[key], dict):
            current = current[key]
        else:
            # The path is invalid; the function can either silently return here or raise an error
            return

    # Remove the property if it exists in the current dictionary
    if args[-1] in current:
        del current[args[-1]]
