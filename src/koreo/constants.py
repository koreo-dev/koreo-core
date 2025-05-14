API_GROUP = PREFIX = "koreo.dev"

DEFAULT_API_VERSION = "v1beta1"

ACTIVE_LABEL = f"{PREFIX}/active"

LAST_APPLIED_ANNOTATION = f"{PREFIX}/last-applied-configuration"

DEFAULT_CREATE_DELAY = 30
DEFAULT_DELETE_DELAY = 15
DEFAULT_LOAD_RETRY_DELAY = 30
DEFAULT_PATCH_DELAY = 30

KOREO_DIRECTIVE_KEYS: set[str] = {
    "x-koreo-compare-as-set",
    "x-koreo-compare-as-map",
    "x-koreo-compare-last-applied",
}


PLURAL_LOOKUP_NEEDED = "+++missing plural+++"
