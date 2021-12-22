#!/usr/bin/env python

import argparse
import re
import ast
import pathlib
import errno
import os
from typing import Dict, List, Set, Tuple


def is_name(node, id) -> bool:
    return isinstance(node, ast.Name) and node.id == id


def is_const(node, val) -> bool:
    return isinstance(node, ast.Constant) and node.value == val


def get_funcs(root: ast.Module) -> List[ast.FunctionDef]:
    funcs = []
    for stmt in root.body:
        if isinstance(stmt, ast.FunctionDef):
            funcs.append(stmt)
    return funcs


def is_pd_attr(node, attr) -> bool:
    return (
        isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "pd"
        and node.attr == attr
    )


def find_main(root: ast.Module) -> List[ast.stmt]:
    for stmt in root.body:
        if (
            isinstance(stmt, ast.If)
            and is_name(stmt.test.left, "__name__")
            and isinstance(stmt.test.ops[0], ast.Eq)
            and is_const(stmt.test.comparators[0], "__main__")
        ):
            return stmt.body
    raise ValueError("main block not found")


def process_local_func_call(
    fn_map: Dict[str, List[ast.stmt]], fn_name: str, debug: bool
) -> Tuple[Set[str], Set[str]]:
    csvs = set()
    outputs = set()
    fn_body = fn_map[fn_name]
    for stmt in fn_body:
        if isinstance(stmt, ast.Assign):
            a, b = process_expr(fn_map, stmt.value, debug)
            csvs = csvs.union(a)
            outputs = outputs.union(b)
            continue
        elif isinstance(stmt, ast.Return):
            a, b = process_expr(fn_map, stmt.value, debug)
            csvs = csvs.union(a)
            outputs = outputs.union(b)
            continue
        elif isinstance(stmt, ast.For) or isinstance(stmt, ast.With):
            a, b = process_expr(fn_map, stmt, debug)
            csvs = csvs.union(a)
            outputs = outputs.union(b)
            continue
        s = ast.dump(stmt)
        if "read_csv" in s:
            raise ValueError("unhandled node %s" % ast.dump(stmt, indent=4))
    return csvs, outputs


def process_expr(
    fn_map: Dict[str, List[ast.stmt]], expr, debug: bool
) -> Tuple[Set[str], Set[str]]:
    csvs = set()
    outputs = set()
    if debug:
        print(ast.dump(expr, indent=4))
    for node in ast.walk(expr):
        if (
            isinstance(node, ast.Constant)
            and type(node.value) is str
            and node.value.endswith(".csv")
        ):
            csvs.add(node.value)
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute) and node.func.attr == "to_csv":
                if (
                    isinstance(node.args[0], ast.Call)
                    and is_name(node.args[0].func, "data_file_path")
                    and isinstance(node.args[0].args[0], ast.Constant)
                ):
                    outputs.add(node.args[0].args[0].value)
                else:
                    raise ValueError("unhandled node %s" % ast.dump(node, indent=4))
        elif isinstance(node, ast.Name):
            if node.id in fn_map:
                a, b = process_local_func_call(fn_map, node.id, debug)
                csvs = csvs.union(a)
                outputs = outputs.union(b)
    return csvs, outputs


def detect_script_input_output(
    q: pathlib.Path, debug: bool
) -> Tuple[List[str], List[str]]:
    csvs = set()
    outputs = set()
    with q.open() as f:
        root = ast.parse(f.read(), q.name)
        fn_map = dict()
        for fn in get_funcs(root):
            fn_map[fn.name] = fn.body
        main = find_main(root)
        for stmt in main:
            a, b = process_expr(fn_map, stmt, debug)
            csvs = csvs.union(a)
            outputs = outputs.union(b)
    inputs = []
    for name in csvs:
        if name not in outputs:
            inputs.append(name)
    return sorted(inputs), sorted([name.split("/")[1] for name in outputs])


def write_make_rules(
    all: bool,
    dir: pathlib.Path,
    scripts: List[Tuple[str, List[str], List[str]]],
    dependencies: List[pathlib.Path] = [],
):
    scripts.sort(key=lambda x: x[0])
    dir_var = "DATA_%s_DIR" % dir.name.upper()
    with open(dir / "data.d", "w") as f:
        for script, inputs, outputs in scripts:
            targets = " ".join(["$(%s)/%s" % (dir_var, name) for name in outputs])
            if all:
                f.write("$(BUILD_DIR)/.fuse-all: %s\n\n" % targets)
            f.write(
                "%s: %s %s | $(%s)\n\tpython %s\n\n"
                % (
                    targets,
                    "$(MD5_DIR)/%s.md5" % (dir / script),
                    " ".join(
                        ["data/%s" % name for name in inputs]
                        + [str(p) for p in dependencies]
                    ),
                    dir_var,
                    dir / script,
                )
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Analyze Python scripts and generate Make rules."
    )
    parser.add_argument(
        "scripts_dir",
        type=pathlib.Path,
        metavar="SCRIPTS_DIR",
        help=(
            "Analyze Python scripts in this folder and output Make rules to SCRIPTS_DIR/data.d. "
            "Note that subfolders are ignored."
        ),
    )
    parser.add_argument(
        "-e",
        "--exclude",
        action="append",
        type=pathlib.Path,
        default=[],
        help="exclude this script from generated make file.",
    )
    parser.add_argument(
        "-a",
        "--all",
        action="store_true",
        help="add rule targets to all's dependencies",
    )
    parser.add_argument(
        "-p",
        "--pattern",
        action="store",
        type=str,
        default="",
        help="only analyze file whose name match this Regexp pattern",
    )
    parser.add_argument(
        "-d", "--debug", action="store_true", help="print debug information"
    )
    parser.add_argument(
        "--dependency",
        action="append",
        type=pathlib.Path,
        default=[],
        help="include this dependency in each rule",
    )
    args = parser.parse_args()
    if not args.scripts_dir.exists():
        raise FileNotFoundError(
            errno.ENOENT, os.strerror(errno.ENOENT), args.scripts_dir
        )
    if not args.scripts_dir.is_dir():
        raise NotADirectoryError(
            errno.ENOTDIR, os.strerror(errno.ENOTDIR), args.scripts_dir
        )
    pat = None
    if args.pattern != "":
        pat = re.compile(args.pattern)

    scripts = []
    for q in args.scripts_dir.glob("*.py"):
        if q in args.exclude:
            continue
        if pat is not None and pat.search(q.name) is None:
            continue
        try:
            inputs, outputs = detect_script_input_output(q, args.debug)
        except ValueError as e:
            if str(e) == "main block not found":
                print("WARNING: main block not found in %s" % q)
                continue
            raise
        scripts.append((q.name, inputs, outputs))

    write_make_rules(args.all, args.scripts_dir, scripts, args.dependency)
