from lark import Tree, Token


def extract_argument_structure(compiled: Tree) -> set[str]:
    var_map: set[str] = set()
    for thing in compiled.iter_subtrees():
        if thing.data == "member_dot":
            member_dot: str = _process_member_dot(thing)
            var_map.add(member_dot)

        if thing.data == "member_index":
            member_index: str = _process_member_index(thing)
            var_map.add(member_index)

    return var_map


def _process_member_dot(tree: Tree):
    if len(tree.children) != 2:
        # TODO: Not sure this is possible?
        raise Exception(f"UNKNOWN MEMBER_DOT LENGTH! {len(tree.children)}: {tree}")

    # print(f"{len(tree.children)}: {tree.children[0]}")

    terminal = f"{tree.children[1]}"

    root: Tree = tree.children[0].children[0]

    if root.data == "member_dot":
        return f"{_process_member_dot(root)}.{terminal}"

    if root.data == "member_index":
        return f"{_process_member_index(root)}.{terminal}"

    if root.data == "member_dot_arg":
        return f"{_process_member_dot_arg(root)}.{terminal}"

    if root.data == "primary":
        return f"{_process_primary(root)}.{terminal}"

    # TODO: Is this possible?
    raise Exception(f"UNKNOWN MEMBER_DOT ROOT TYPE! {root}")


def _process_member_dot_arg(tree: Tree):
    if len(tree.children) != 3:
        # TODO: Not sure this is possible?
        raise Exception(f"UNKNOWN MEMBER_DOT_ARG LENGTH! {len(tree.children)}: {tree}")

    # print(f"{len(tree.children)}: {tree.children[0]}")

    terminal = f"{tree.children[1]}"

    root: Tree = tree.children[0].children[0]

    if root.data == "member_dot":
        return f"{_process_member_dot(root)}.{terminal}"

    if root.data == "member_index":
        return f"{_process_member_index(root)}.{terminal}"

    if root.data == "member_dot_arg":
        return f"{_process_member_dot_arg(root)}.{terminal}"

    if root.data == "primary" or root.data == "ident":
        return f"{_process_primary(root)}.{terminal}"

    # TODO: Is this possible?
    raise Exception(f"UNKNOWN MEMBER_DOT_ARG ROOT TYPE! {root}")


def _process_member_index(tree: Tree):
    if len(tree.children) != 2:
        # TODO: Not sure this is possible?
        raise Exception(f"UNKNOWN MEMBER_INDEX LENGTH! {len(tree.children)}: {tree}")

    # print(f"{len(tree.children)}: {tree.children[0]}")

    terminal: Tree = tree.children[1]

    if terminal.data == "primary":
        terminal_value: str = _process_primary(terminal)

    elif terminal.data == "expr":
        terminal_value = None
        while terminal and terminal.children:
            if terminal.data == "primary":
                terminal_value = _process_primary(terminal)
                break

            terminal: Tree = terminal.children[0]

        if not terminal_value:
            raise Exception(f"CAN NOT PROCESS MEMBER_INDEX terminal expr: {terminal}")

    else:
        raise Exception(f"UNKNOWN MEMBER_INDEX terminal TYPE: {terminal}")

    root: Tree = tree.children[0].children[0]

    if root.data == "member_dot":
        root_value = _process_member_dot(root)
    elif root.data == "member_index":
        root_value = _process_member_index(root)
    elif root.data == "member_dot_arg":
        root_value = _process_member_dot_arg(root)
    elif root.data == "primary":
        root_value: str = _process_primary(root)
    else:
        raise Exception(f"UNKNOWN MEMBER_INDEX root TYPE: {root}")

    return f"{root_value}.{terminal_value}"


def _process_primary(tree: Tree) -> str:
    if len(tree.children) != 1:
        raise Exception(f"UNKNOWN PRIMARY LENGTH! {len(tree.children)}: {tree}")

    primary: Tree = tree.children[0]

    # print(f"{len(tree.children)}: {primary}")

    if primary.data == "ident":
        return f"{primary.children[0]}"

    if primary.data == "literal":
        literal: Token = primary.children[0]
        if literal.type == "INT_LIT":
            return literal.value
        return literal.strip(literal[0])

    raise Exception(f"UNKNOWN PRIMARY DATA TYPE! {primary}")
