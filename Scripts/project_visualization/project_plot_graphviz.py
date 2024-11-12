import os
import ast
import sys
import site
from graphviz import Digraph
import subprocess


def visualize_dependencies(root_dir, output_dir):

    def extract_imports(file_path):
        imports = []
        with open(file_path, 'r') as file:
            tree = ast.parse(file.read(), file_path)
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        imports.append(alias.name)
                elif isinstance(node, ast.ImportFrom):
                    imports.append(node.module)
        return imports

    def is_third_party_module(module_name):
        # Check if the module is located in site-packages or dist-packages
        site_packages = site.getsitepackages() + [site.getusersitepackages()]
        for site_package in site_packages:
            if isinstance(site_package, str):
                if module_name.startswith(site_package):
                    return True
            elif isinstance(site_package, list):
                for sp in site_package:
                    if module_name.startswith(sp):
                        return True
        return False


    graph = Digraph(format='png')
    graph.attr('node', shape='circle')

    def add_files(directory):
        for item in os.listdir(directory):
            full_path = os.path.join(directory, item)
            if os.path.isfile(full_path) and full_path.endswith('.py'):
                module_name = os.path.splitext(item)[0]
                imports = extract_imports(full_path)
                for imp in imports:
                    if not is_third_party_module(imp) and imp != '__future__':
                        graph.edge(module_name, imp)
            elif os.path.isdir(full_path):
                add_files(full_path)

    add_files(root_dir)
    graph.render('dependency_graph', view=True)


##pyreverse
def generate_uml(project_path, output_dir=r"C:\Daten\Kiki\ProgrammingStuff\visualiszation"):
    """
    Generate UML diagrams for a Python project using Pyreverse.

    :param project_path: Path to the Python project.
    :param output_dir: Directory where the UML diagrams will be saved.
    """
    # Ensure the output directory exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Change to the project directory
    os.chdir(project_path)

    # Run pyreverse to generate UML diagrams
    print("Generating UML diagrams...")
    subprocess.run(["pyreverse", "-o", "png", "-p", "project_diagrams", "."], check=True)

    # Move the generated diagrams to the output directory
    for file in ["classes_project_diagrams.png", "packages_project_diagrams.png"]:
        if os.path.exists(file):
            os.rename(file, os.path.join(output_dir, file))
            print(f"{file} generated and moved to {output_dir}")

if __name__ == "__main__":
    # Path to your Python project
    output_dir=r"C:\Daten\Kiki\ProgrammingStuff\visualiszation"
    project_root = r'C:\Daten\Kiki\ProgrammingStuff\t_p_etc_recorder_v2\Scripts'
    visualize_dependencies(project_root, output_dir)

