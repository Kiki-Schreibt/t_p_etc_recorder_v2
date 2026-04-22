import os
import sys
import mkdocs_gen_files

# Wichtig: macht src importierbar
sys.path.insert(0, "src")

SRC_DIR = "src"
API_DIR = "api"
print("GEN FILES RUNNING")
def prettify(name: str) -> str:
    return name.replace("_", " ").title()

for root, dirs, files in os.walk(SRC_DIR):
    # Skip cache
    if "__pycache__" in root:
        continue

    rel_path = os.path.relpath(root, SRC_DIR)
    rel_path = "" if rel_path == "." else rel_path

    doc_path = os.path.join(API_DIR, rel_path)
    module_path = rel_path.replace(os.sep, ".")

    # --- .pages file für Navigation ---
    pages_file = os.path.join(doc_path, ".pages")
    with mkdocs_gen_files.open(pages_file, "w") as nav:
        title = prettify(os.path.basename(root)) if rel_path else "API"
        nav.write(f"title: {title}\n")

    # --- index.md für Ordner ---
    index_file = os.path.join(doc_path, "index.md")
    with mkdocs_gen_files.open(index_file, "w") as f:
        if module_path:
            f.write(f"# {prettify(module_path.split('.')[-1])}\n\n")
            f.write(f"::: {module_path}\n")
            f.write("    options:\n")
            f.write("      show_submodules: true\n")

    # --- einzelne Module ---
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
