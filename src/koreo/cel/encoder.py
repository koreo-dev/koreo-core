from typing import Any

CEL_PREFIX = "="


def encode_cel(value):
    if isinstance(value, dict):
        return f"{{{ ",".join(
            f'"{key.replace('"', '\"')}":{encode_cel(value)}'
            for key, value in value.items()
        ) }}}"

    if isinstance(value, list):
        return f"[{ ",".join(encode_cel(item) for item in value) }]"

    if isinstance(value, bool):
        return "true" if value else "false"

    if _encode_plain(value):
        return f"{value}"

    if not value:
        return '""'

    if not value.startswith(CEL_PREFIX):
        return f'"{ value.replace('"', '\"') }"'  # fmt: skip

    return value.lstrip(CEL_PREFIX)


def encode_cel_template(template_spec: dict):
    return f"{{{','.join([f'"{field}": {expression}'
     for field, expression in _encode_template_dict("", template_spec)
     ])}}}"


def _encode_template_dict(base: str, template_spec: dict):
    output: list[tuple[str, Any]] = []

    for field, expression in template_spec.items():
        safe_field = field.replace('"', "'")

        field_name = safe_field
        if base:
            # TODO: QUESTION: I wonder if this should instead just check for
            # '.'?  Maybe we should only allow json-path from the "root" level
            # and not from nested fields?
            if base.endswith("labels") or base.endswith("annotations"):
                field_name = f"{base}['{safe_field}']"
                print(field_name)
            else:
                field_name = f"{base}.{safe_field}"

        if isinstance(expression, dict):
            output.extend(_encode_template_dict(field_name, expression))
        else:
            output.append((field_name, encode_cel(expression)))

    return output


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
