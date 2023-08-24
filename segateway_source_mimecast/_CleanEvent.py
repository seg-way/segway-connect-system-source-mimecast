import orjson


def CleanEvent(source_dict: dict):
    """
    Delete keys with the value ``None``  or ```` (empty) string in a dictionary, recursively.
    Remove empty list and dict objects

    This alters the input so you may wish to ``copy`` the dict first.
    """
    # For Python 3, write `list(d.items())`; `d.items()` won’t work
    # For Python 2, write `d.items()`; `d.iteritems()` won’t work
    for key, value in list(source_dict.items()):
        if value is None:
            del source_dict[key]
        elif isinstance(value, str) and value in ("", "None", "none"):
            del source_dict[key]
        elif isinstance(value, str):
            if value.endswith("\n"):
                value = value.strip("\n")

            if value.startswith('{"'):
                try:
                    value = orjson.loads(value)
                    CleanEvent(value)
                    source_dict[key] = value
                except orjson.JSONDecodeError:
                    pass
        elif isinstance(value, dict) and not value:
            del source_dict[key]
        elif isinstance(value, dict):
            CleanEvent(value)
        elif isinstance(value, list) and not value:
            del source_dict[key]
    return source_dict  # For convenience
