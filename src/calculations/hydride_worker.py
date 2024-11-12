import csv
import json
import re

from src.config_connection_reading_management.connections_and_logger import AppLogger
from src.standard_paths import standard_hydride_data_base_path, standard_periodic_table_path


def parse_chemical_formula(hydride_name):
    # Regular expression to match elements and optional quantities
    pattern = r'([A-Z][a-z]*)(\d*)'
    parts = re.findall(pattern, hydride_name)

    # Convert quantity to integer, default to 1 if not present
    result = {}
    for element, quantity in parts:
        result[element] = int(quantity) if quantity else 1

    return result


class MetalHydrideDatabase:

    def __init__(self, hydride_data_base_path=standard_hydride_data_base_path):
        self.hydride_data_base_path = hydride_data_base_path
        self.hydride_data_base = []
        self.load_hydride_database()
        self.logger = AppLogger().get_logger(__name__)
        self.periodic_table_of_elements = PeriodicTableOfElements()

    def load_hydride_database(self):
        with open(self.hydride_data_base_path, 'r') as file:
            self.hydride_data_base = json.load(file)

    def get_enthalpy_entropy(self, hydride_to_grab):
        """
        Choose hydride for test
        Parameters:
            hydride_to_grab: hydride enthalpy and entropy will be returned from
        Returns:
            enthalpy in J mol^-1: float
            entropy in J mol^-1 K^-1 : float
        """
        # Search for hydride
        hydride_to_grab = self._normalize_hydride_string(hydride_to_grab)

        for hydride in self.hydride_data_base:
            if hydride['Hydride'] == hydride_to_grab:
                self.logger.info(f"Found {hydride_to_grab}: Enthalpy = {hydride['Enthalpy']}, Entropy = {hydride['Entropy']}")
                enthalpy = float(hydride['Enthalpy'])
                entropy = float(hydride['Entropy'])
                return enthalpy, entropy

        self.logger.error(f" {hydride_to_grab}: is not in data base yet")
        return None, None

    def hydride_adder(self, hydride_to_add_name, enthalpy, entropy):
        """
        Adds a new hydride to the database, if it does not already exist.
        Parameters:
            hydride_to_add_name: Name of the hydride to add.
            enthalpy: Enthalpy of the hydride in J mol^-1.
            entropy: Entropy of the hydride in J mol^-1 K^-1.
        """
        # Convert enthalpy and entropy values
        enthalpy *= 1e3
        entropy *= 1e3

        # Check if the hydride already exists
        for hydride in self.hydride_data_base:
            if hydride['Hydride'] == hydride_to_add_name:
                self.logger.info(f"{hydride_to_add_name} already exists in the database.")
                return

        # Add the new hydride
        hydride_to_add = {
            "Hydride": hydride_to_add_name,
            "Enthalpy": enthalpy,
            "Entropy": entropy
        }
        self.hydride_data_base.append(hydride_to_add)
        self.logger.info(f"Added {hydride_to_add_name} to the database.")

        # Save the updated data
        with open(self.hydride_data_base_path, 'w') as file:
            json.dump(self.hydride_data_base, file, indent=4)
        self.load_hydride_database()

    def hydride_remover(self, hydride_to_remove_name):
        """
        Removes a specified hydride from the database.

        Parameters:
            hydride_to_remove_name (str): The name of the hydride to remove.
        """
        # Iterate through the database and find the hydride to remove
        for hydride in self.hydride_data_base:
            if hydride['Hydride'] == hydride_to_remove_name:
                self.hydride_data_base.remove(hydride)
                self.logger.info(f"Removed {hydride_to_remove_name} from the database.")
                break
        else:
            self.logger.info(f"{hydride_to_remove_name} not found in the database.")
            return

        # Save the updated database
        with open(self.hydride_data_base_path, 'w') as file:
            json.dump(self.hydride_data_base, file, indent=4)
        self.load_hydride_database()

    def update_hydride_info(self, hydride_name, new_info):
        """
        Update or add additional information for a specific hydride.

        Parameters:
            hydride_name (str): The name of the hydride to update.
            new_info (dict): A dictionary containing new attributes and their values.
        """
        # Search for the hydride and update its info
        for hydride in self.hydride_data_base:
            if hydride['Hydride'] == hydride_name:
                for key, value in new_info.items():
                    hydride[key] = value
                self.logger.info(f"Updated {hydride_name} with new information.")
                break
        else:
            self.logger.info(f"{hydride_name} not found in the database. Adding new hydride.")
            new_hydride = {"Hydride": hydride_name}
            new_hydride.update(new_info)
            self.hydride_data_base.append(new_hydride)

        # Save the updated database
        with open(self.hydride_data_base_path, 'w') as file:
            json.dump(self.hydride_data_base, file, indent=4)
        self.load_hydride_database()

    def get_capacity(self, hydride_name):
        material = self._normalize_hydride_string(hydride_name)
        hydride_mass, hydrogen_mass_in_hydride = self.get_molar_mass_hydride(hydride_name=hydride_name, return_hydrogen_mass=True)
        if hydrogen_mass_in_hydride and hydride_mass:
            theoretical_capacity = hydrogen_mass_in_hydride/hydride_mass * 100
            self.logger.info(f"Capacity of {material} = {theoretical_capacity} wt-%")
            return theoretical_capacity
        else:
            return None

    def get_molar_mass_hydride(self, hydride_name, return_hydrogen_mass=False):
        material = self._normalize_hydride_string(hydride_name)
        # Using findall to extract elements and counts
        element_dict = self.extract_elements(material)
        hydride_mass = 0
        hydrogen_mass_in_hydride = None
        for element, quantity in element_dict.items():
            atomic_mass = self.periodic_table_of_elements.atomic_mass_grabber(element)
            # TODO: Make this work also for hydrides that have Hydrogen left in structure in dehydrogenated state
            if atomic_mass:
                mass_in_hydride = atomic_mass * quantity
                hydride_mass += mass_in_hydride
                if element == "H":
                    hydrogen_mass_in_hydride = atomic_mass * quantity

        if return_hydrogen_mass:
            return hydride_mass, hydrogen_mass_in_hydride
        else:
            return hydride_mass

    def get_density(self, hydride_to_grab, de_hyd_state="Dehydrogenated"):
        """
        Choose hydride for test
        Parameters:
            hydride_to_grab: hydride enthalpy and entropy will be returned from
        Returns:
            density: float
        """
        material = hydride_to_grab
        density = None
        # Search for hydride
        hydride_to_grab = self._normalize_hydride_string(hydride_to_grab)

        for hydride in self.hydride_data_base:
            if hydride['Hydride'] == hydride_to_grab:
                self.logger.info(f"Found {hydride_to_grab}: Density = {hydride['Density'][de_hyd_state]}")
                density = float(hydride['Density'][de_hyd_state])
                return density

        if not density and len(material) <= 2:
            for element in self.periodic_table_of_elements.periodic_table:
                if element['Symbol'] == material:
                    self.logger.info(f"Found {hydride_to_grab}: Density = {element['Density']}")

                    density = element['Density']
                    return density

        self.logger.error(f" {hydride_to_grab}: is not in data base yet")
        return None

    def get_bulk_conductivity(self, hydride_to_grab, de_hyd_state="Hydrogenated"):
        """
        Choose hydride for test
        Parameters:
            hydride_to_grab: hydride enthalpy and entropy will be returned from
        Returns:
            conductivity: float
        """

        # Search for hydride
        hydride_to_grab = self._normalize_hydride_string(hydride_to_grab)

        for hydride in self.hydride_data_base:
            if hydride['Hydride'] == hydride_to_grab:
                self.logger.info(f"Found {hydride_to_grab}: Bulk conductivity = {hydride['Conductivity_Bulk'][de_hyd_state]}")
                value = float(hydride['Conductivity_Bulk'][de_hyd_state])
                return value

        self.logger.error(f" {hydride_to_grab}: is not in data base yet")
        return None

    @staticmethod
    def _normalize_hydride_string(s):
        """
        Takes a string with metal hydride system like MgH2 + 3 wt-% Fe / Mg and turns it into standard form Mg1H2
        :param s: string with chemical formular
        :return:
        """
        match = re.search(r'^[^+/]*', s)
        if match:
            hydride_name = match.group(0).strip()
            parsed_hydride_name = parse_chemical_formula(hydride_name=hydride_name)
            normalized_hydride_name = ''.join([f'{key}{value}' for key, value in parsed_hydride_name.items()])
            return normalized_hydride_name
        return ''  # Return an empty string if no match is found

    @staticmethod
    def extract_elements(formula):
        pattern = r"([A-Z][a-z]*)(\d*)"
        elements = re.findall(pattern, formula)
        return {elem: int(count) if count else 1 for elem, count in elements}


class PeriodicTableOfElements:

    def __init__(self, periodic_table_path=standard_periodic_table_path):
        self.logger = AppLogger().get_logger(__name__)
        self.periodic_table_path = periodic_table_path
        self.load_periodic_table()

    def load_periodic_table(self):
        with open(self.periodic_table_path, 'r') as file:
            self.periodic_table = json.load(file)

    def atomic_mass_grabber(self, element_to_grab):
        """
        Parameters:
            element_to_grab: element the atomic mass is wanted from
        Returns:
            atomic_mass: atomic mass of the input element in u (as a float, if possible)
        """
        # Search for element
        for element in self.periodic_table:
            if element['Symbol'] == element_to_grab:
                try:
                    # Attempt to convert atomic mass to float
                    atomic_mass = float(element['AtomicMass'])
                    self.logger.info(f"Found {element_to_grab}: Mass = {atomic_mass} u")
                    return atomic_mass
                except ValueError:
                    # Log an error if conversion fails
                    self.logger.error(f"Atomic mass of {element_to_grab} is not a valid number.")
                    return None

        # Log error if element is not found
        self.logger.error(f"{element_to_grab} is not an element.")
        return None


def create_periodic_table_of_elements():
    ## create periodic_table_of_elements.json
    input_file_path = 'path/to/periodic_table_of_elements.txt'

    ## Replace 'output_data.json' with your desired output file name
    output_file_path = standard_periodic_table_path.replace('.txt', '.json')

    ## Read and parse the .txt file
    data = []
    with open(input_file_path, newline='') as file:
        reader = csv.DictReader(file)
        for row in reader:
            data.append(row)

    # Write the data to a .json file
    with open(output_file_path, 'w', encoding='utf-8') as jsonfile:
        json.dump(data, jsonfile, ensure_ascii=False, indent=4)

    print(f"Data successfully written to {output_file_path}")


def create_hydride_data_base():
    #DataBase Creator
    data = """
    Hydride,Enthalpy,Entropy
    Fe1H1,42300,52
    Mg1H2,74701,134.7
    Mg2Al3H4,62700,123
    Mg2Fe1H6,79211.124243,137.188755
    Mg2Ni1H4,61858,118.9
    """


    # Splitting data into lines and then into columns
    lines = data.strip().split('\n')
    headers = lines[0].split(',')
    hydride_data = []

    for line in lines[1:]:
        values = line.split(',')
        hydride_data.append(dict(zip(headers, values)))
    # Saving data to a JSON file at the specified path
    with open(standard_hydride_data_base_path, 'w') as file:
        json.dump(hydride_data, file, indent=4)


def test_metal_hydride_database_and_periodic_table():
    # Initialize the classes
    hydride_db = MetalHydrideDatabase()
    periodic_table = PeriodicTableOfElements()

    # Test adding a new hydride
    hydride_to_add = "TestHydride"
    hydride_db.hydride_adder(hydride_to_add, 1000, 50)

    # Test grabbing hydride data
    enthalpy, entropy = hydride_db.hydride_grabber(hydride_to_add)
    print(f"Enthalpy: {enthalpy}, Entropy: {entropy}")

    # Test updating hydride information
    new_info = {"AdditionalInfo": "Test info"}
    hydride_db.update_hydride_info(hydride_to_add, new_info)

    # Test removing a hydride
    hydride_db.hydride_remover(hydride_to_add)

    # Test grabbing atomic mass
    element_to_grab = "H"  # Hydrogen
    atomic_mass = periodic_table.atomic_mass_grabber(element_to_grab)
    print(f"Atomic Mass of {element_to_grab}: {atomic_mass} u")

     # Example usage
    formula = "Mg2FeH6"
    parsed_formula = parse_chemical_formula(formula)
    print(formula,"parsed into", parsed_formula)  # Output: {'Mg': 1, 'H': 2}


    print("Test completed successfully.")

# Run the test function
if __name__ == "__main__":
    #test_metal_hydride_database_and_periodic_table()
    # Example usage
    string = "MgH2"
    mh_data = MetalHydrideDatabase()
    print("density " + str(mh_data.get_density(string)))
    print("conductivity " + str(mh_data.get_bulk_conductivity(string)))
    print("capacity " + str(mh_data.get_capacity(string)))





