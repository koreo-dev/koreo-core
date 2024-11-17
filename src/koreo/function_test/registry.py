from collections import defaultdict


__test_function_index = defaultdict(str)
__function_test_index = defaultdict(set[str])


def index_test_function(test: str, function: str):
    prior_function = __test_function_index[test]

    if prior_function == function:
        return

    if test in __function_test_index[prior_function]:
        __function_test_index[prior_function].remove(test)

    __function_test_index[function].add(test)

    __test_function_index[test] = function


def get_function_tests(function: str) -> list[str]:
    return list(__function_test_index[function])


def _reset_registry():
    global __test_function_index, __function_test_index
    __test_function_index = defaultdict(str)
    __function_test_index = defaultdict(set[str])
