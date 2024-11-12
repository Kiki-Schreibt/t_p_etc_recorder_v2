import sys
import os


def add_source_folders(folder_names=None):
    if not folder_names:
        folder_names = ["", "calculations",
                        "config_connection_reading_management",
                        "GUI", "manual_data_import",
                        "meta_data", "../Scripts"]
    exclude = ["testing_classes", "uml_diagrams", "project_visualization"]

    base_dir = os.path.dirname(os.path.dirname(__file__))
    for name in folder_names:
        folder_path = os.path.abspath(os.path.join(base_dir, "src", name))

        if os.path.exists(folder_path):
            for root, dirs, files in os.walk(folder_path):
                # Skip excluded directories
                dirs[:] = [d for d in dirs if d not in exclude]

                if "pycache" not in root and "__old" not in root:
                    if root not in sys.path:
                        sys.path.append(root)
        else:
            print(f"Warning: {folder_path} does not exist")
    #print(sys.path)



if __name__ == '__main__':

    name = ("calculations",  "config_connection_reading_management", "GUI", "manual_data_import",  "meta_data")
    add_source_folders()
