from collections import defaultdict

from koreo.cache import get_resource_from_cache

from .structure import ResourceTemplate

__template_name_cache_key_index = defaultdict(str)


def index_resource_template(cache_key: str, template_key: str):
    __template_name_cache_key_index[template_key] = cache_key


def get_resource_template(template_key: str) -> ResourceTemplate | None:
    cache_key = __template_name_cache_key_index.get(template_key)
    if not cache_key:
        return None

    return get_resource_from_cache(resource_class=ResourceTemplate, cache_key=cache_key)
