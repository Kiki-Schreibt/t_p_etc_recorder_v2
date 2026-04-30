import os
import sys
import mkdocs_gen_files

sys.path.insert(0, "src")

SRC_DIR = "src"
API_DIR = "api"

print("GEN FILES RUNNING")


def prettify(name: str) -> str:
    return name.replace("_", " ").title()


def list_submodules(root_path):
    """Return subpackages + modules (no __init__)"""
    items = []
    for entry in sorted(os.listdir(root_path)):
        full = os.path.join(root_path, entry)
        if entry == "__pycache__":
            continue
        if os.path.isdir(full):
            items.append((entry, "dir"))
        elif entry.endswith(".py") and entry != "__init__.py":
            items.append((entry[:-3], "file"))
    return items


for root, dirs, files in os.walk(SRC_DIR):
    if "__pycache__" in root:
        continue

    rel_path = os.path.relpath(root, SRC_DIR)
    rel_path = "" if rel_path == "." else rel_path

    doc_path = os.path.join(API_DIR, rel_path).lower()
    module_path = rel_path.replace(os.sep, ".")

    os.makedirs(doc_path, exist_ok=True)

    # --- navigation (.pages) ---
    pages_file = os.path.join(doc_path, ".pages")
    with mkdocs_gen_files.open(pages_file, "w") as nav:
        title = prettify(os.path.basename(root)) if rel_path else "API"
        nav.write(f"title: {title}\n")

    # --- index.md ---
    index_file = os.path.join(doc_path, "index.md")
    subitems = list_submodules(root)

    with mkdocs_gen_files.open(index_file, "w") as f:
        if not module_path:
            # ROOT INDEX
            f.write("# API Reference\n\n")
            f.write("Welcome to the API documentation.\n\n")
            f.write("## Modules\n\n")

            if subitems:
                for name, typ in subitems:
                    if typ == "dir":
                        f.write(f"- [{prettify(name)}]({name}/)\n")
                    else:
                        f.write(f"- [{prettify(name)}]({name}.md)\n")
            else:
                f.write("_No modules found._\n")

        else:
            # PACKAGE INDEX
            title = prettify(module_path.split(".")[-1])
            f.write(f"# {title}\n\n")

            f.write(f"::: {module_path}\n\n")

            if subitems:
                f.write("## Contents\n\n")
                for name, typ in subitems:
                    if typ == "dir":
                        f.write(f"- [{prettify(name)}]({name}/)\n")
                    else:
                        f.write(f"- [{prettify(name)}]({name}.md)\n")

    # --- module files ---
    for file in files:
        if not file.endswith(".py") or file == "__init__.py":
            continue

        module_name = file[:-3]

        if module_path:
            import_path = f"{module_path}.{module_name}"
        else:
            import_path = module_name

        file_path = os.path.join(doc_path, f"{module_name}.md")

        with mkdocs_gen_files.open(file_path, "w") as f:
            f.write(f"# {prettify(module_name)}\n\n")
            f.write(f"::: {import_path}\n")
            f.write("    options:\n")
            f.write("      show_root_heading: true\n")
            f.write("      show_source: true\n")
