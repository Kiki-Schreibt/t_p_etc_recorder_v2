
import os
import sys
from pathlib import Path
import mkdocs_gen_files


EXCLUDE = {
    "__init__",
    "__main__",
    "conftest",
}

EXCLUDE_DIRS = {
    "__pycache__",
    "tests",
    "migrations",
}

GROUPS = {
    "Core": ["core", "client", "main"],
    "Models": ["models", "schemas"],
    "Services": ["services"],
    "Utils": ["utils", "helpers"],
}


sys.path.insert(0, "src")

SRC_DIR = Path("src")
API_INDEX = Path("api/index.md")


def is_valid_module(path: Path):
    return (
        path.suffix == ".py"
        and path.stem not in EXCLUDE
    )


def collect_modules():
    modules = []

    for path in SRC_DIR.rglob("*.py"):
        if any(part in EXCLUDE_DIRS for part in path.parts):
            continue
        if not is_valid_module(path):
            continue

        rel = path.relative_to(SRC_DIR).with_suffix("")
        import_path = ".".join(rel.parts)
        modules.append(import_path)

    return sorted(modules)


def group_modules(modules):
    grouped = {k: [] for k in GROUPS}
    grouped["Other"] = []

    for mod in modules:
        placed = False
        for group, keys in GROUPS.items():
            if any(part in mod for part in keys):
                grouped[group].append(mod)
                placed = True
                break
        if not placed:
            grouped["Other"].append(mod)

    return grouped


modules = collect_modules()
grouped = group_modules(modules)

with mkdocs_gen_files.open(API_INDEX, "w") as f:
    f.write("# API Reference\n\n")
    f.write("Auto-generated API documentation.\n\n")

    for group, items in grouped.items():
        if not items:
            continue

        f.write(f"## {group}\n\n")

        for mod in items:
            doc_path = f"api/{mod.replace('.', '/')}.md"

            # write individual page
            with mkdocs_gen_files.open(doc_path, "w") as mf:
                mf.write(f"# {mod}\n\n")
                mf.write(f"::: {mod}\n")

            # link from index
            f.write(f"### [{mod}]({mod.replace('.', '/')}.md)\n\n")
