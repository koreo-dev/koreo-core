CEL_PREFIX = "="


def encode_cel(value):
    if isinstance(value, dict):
        return f"{{{ ",".join(
            f'"{key.replace('"', '\"')}":{encode_cel(value)}'
            for key, value in value.items()
        ) }}}"

    if isinstance(value, list):

        return f"[{ ",".join(encode_cel(list_value) for list_value in value) }]"

    if _encode_plain(value):
        return value

    if not value:
        return '""'

    if not value.startswith(CEL_PREFIX):
        return f'"{ value.replace('"', '\"') }"'  # fmt: skip

    return value.lstrip(CEL_PREFIX)


def _encode_plain(maybe_number) -> bool:
    if not isinstance(maybe_number, str):
        return True

    try:
        int(maybe_number)
        return True
    except ValueError:
        pass

    try:
        float(maybe_number)
        return True
    except ValueError:
        pass

    return False
