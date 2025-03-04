PREFIX = "koreo.dev"

ACTIVE_LABEL = f"{PREFIX}/active"

LAST_APPLIED_ANNOTATION = f"{PREFIX}/last-applied-configuration"

DEFAULT_LOAD_RETRY_DELAY = 30

KOREO_DIRECTIVE_KEYS: set[str] = {
    "x-koreo-compare-as-set",
    "x-koreo-compare-as-map",
    "x-koreo-compare-last-applied",
}
