#hydride_handler.py
"""
Module for managing a metal hydride database and the periodic table of elements.
"""

import csv
import json
import re

from src.infrastructure.utils.standard_paths import (
    standard_hydride_data_base_path,
    standard_periodic_table_path,
    standard_periodic_table_txt,  # assumed to be defined
)

# Use a custom logger if available, otherwise default to the standard logging module.
try:
    import src.infrastructure.core.logger as custom_logging
    logging = custom_logging
except ImportError:
    import logging


def parse_chemical_formula(formula: str) -> dict[str, float | int]:
    """
    Parse a chemical formula string into element counts.
    Supports fractional stoichiometries like 0.85 and 1.5 (and scientific notation).

    Examples:
        "MgH2"            -> {"Mg": 1, "H": 2}
        "LaNi0.85Al0.15"  -> {"La": 1, "Ni": 0.85, "Al": 0.15}
    """
    number = r"(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?"
    pattern = rf"([A-Z][a-z]*)(?:({number}))?"
    parts = re.findall(pattern, formula)

    counts: dict[str, float | int] = {}
    for element, qty in parts:
        value = float(qty) if qty else 1.0
        # Convert to int if it's a whole number (e.g. 2.0 → 2)
        if value.is_integer():
            value = int(value)
        counts[element] = counts.get(element, 0) + value
    return counts


class MetalHydrideDatabase:
    """
    Class for managing a metal hydride database.
    Provides methods to load, update, and retrieve hydride properties.
    """

    def __init__(self, hydride_db_path: str = standard_hydride_data_base_path):
        """
        Initialize the MetalHydrideDatabase.

        Parameters:
            hydride_db_path (str): Path to the hydride database JSON file.
        """
        self.hydride_data_base_path = hydride_db_path
        self.hydride_data_base = []
        self.logger = logging.getLogger(__name__)
        self.load_hydride_database()
        self.periodic_table_of_elements = PeriodicTableOfElements()

    def load_hydride_database(self) -> None:
        """Load the hydride database from a JSON file."""
        with open(self.hydride_data_base_path, "r") as file:
            self.hydride_data_base = json.load(file)

    def get_enthalpy_entropy(self, hydride_name: str) -> tuple:
        """
        Retrieve the enthalpy and entropy for a given hydride.

        Parameters:
            hydride_name (str): The hydride identifier.

        Returns:
            tuple: (enthalpy in J/mol, entropy in J/(mol*K)) or (None, None) if not found.
        """
        normalized = self._normalize_hydride_string(hydride_name)
        for entry in self.hydride_data_base:
            if entry.get("Hydride") == normalized:
                self.logger.info(
                    f"Found {normalized}: Enthalpy = {entry['Enthalpy']}, Entropy = {entry['Entropy']}"
                )
                return float(entry["Enthalpy"]), float(entry["Entropy"])
        self.logger.error(f"{normalized} is not in the database.")
        return None, None

    def hydride_adder(self, hydride_name: str, enthalpy: float, entropy: float) -> None:
        """
        Add a new hydride to the database if it does not already exist.

        Parameters:
            hydride_name (str): Name of the hydride.
            enthalpy (float): Enthalpy in kJ/mol (will be converted to J/mol).
            entropy (float): Entropy in kJ/(mol*K) (will be converted to J/(mol*K)).
        """
        # Convert values from kJ to J
        enthalpy *= 1e3
        entropy *= 1e3

        # Check if the hydride already exists
        for entry in self.hydride_data_base:
            if entry.get("Hydride") == hydride_name:
                self.logger.info(f"{hydride_name} already exists in the database.")
                return

        new_entry = {
            "Hydride": hydride_name,
            "Enthalpy": enthalpy,
            "Entropy": entropy,
        }
        self.hydride_data_base.append(new_entry)
        self.logger.info(f"Added {hydride_name} to the database.")
        self._save_database()

    def hydride_remover(self, hydride_name: str) -> None:
        """
        Remove a specified hydride from the database.

        Parameters:
            hydride_name (str): The name of the hydride to remove.
        """
        for entry in self.hydride_data_base:
            if entry.get("Hydride") == hydride_name:
                self.hydride_data_base.remove(entry)
                self.logger.info(f"Removed {hydride_name} from the database.")
                self._save_database()
                return
        self.logger.info(f"{hydride_name} not found in the database.")

    def update_hydride_info(self, hydride_name: str, new_info: dict) -> None:
        """
        Update or add additional information for a specific hydride.

        Parameters:
            hydride_name (str): The name of the hydride to update.
            new_info (dict): A dictionary containing new attributes and their values.
        """
        for entry in self.hydride_data_base:
            if entry.get("Hydride") == hydride_name:
                entry.update(new_info)
                self.logger.info(f"Updated {hydride_name} with new information.")
                self._save_database()
                return

        self.logger.info(f"{hydride_name} not found in the database. Adding new hydride.")
        new_entry = {"Hydride": hydride_name}
        new_entry.update(new_info)
        self.hydride_data_base.append(new_entry)
        self._save_database()

    def get_capacity(self, hydride_name: str) -> float:
        """
        Calculate the theoretical capacity (wt-%) of a hydride.

        Parameters:
            hydride_name (str): The hydride identifier.

        Returns:
            float: Theoretical capacity in wt-% or None if data is insufficient.
        """
        total_mass, hydrogen_mass = self.get_molar_mass_hydride(
            hydride_name, return_hydrogen_mass=True
        )
        if total_mass and hydrogen_mass:
            capacity = (hydrogen_mass / total_mass) * 100
            self.logger.info(f"Capacity of {hydride_name} = {capacity:.2f} wt-%")
            return capacity
        return None

    def get_molar_mass_hydride(self, hydride_name: str, return_hydrogen_mass: bool = False):
        """
        Calculate the molar mass of a hydride and optionally the mass of hydrogen in it.

        Parameters:
            hydride_name (str): The hydride identifier.
            return_hydrogen_mass (bool): If True, return a tuple (total_mass, hydrogen_mass).

        Returns:
            float or tuple: Total molar mass in u, or a tuple (total_mass, hydrogen_mass).
        """
        normalized = self._normalize_hydride_string(hydride_name)
        element_counts = self.extract_elements(normalized)
        total_mass = 0.0
        hydrogen_mass = 0.0

        for element, count in element_counts.items():
            atomic_mass = self.periodic_table_of_elements.atomic_mass_grabber(element)
            if atomic_mass is not None:
                total_mass += atomic_mass * count
                if element == "H":
                    hydrogen_mass += atomic_mass * count

        if return_hydrogen_mass:
            return total_mass, hydrogen_mass
        return total_mass

    def get_density(self, hydride_name: str, state: str = "Dehydrogenated") -> float:
        """
        Retrieve the density of a hydride for a specified state.

        Parameters:
            hydride_name (str): The hydride identifier.
            state (str): The material state (e.g., "Dehydrogenated").

        Returns:
            float: Density value or None if not found.
        """
        normalized = self._normalize_hydride_string(hydride_name)
        for entry in self.hydride_data_base:
            if entry.get("Hydride") == normalized:
                try:
                    density = float(entry["Density"][state])
                    self.logger.info(f"Found {normalized}: Density = {density}")
                    return density
                except (KeyError, ValueError):
                    self.logger.error(
                        f"Density information for {normalized} in state '{state}' is missing or invalid."
                    )
                    return None

        # If not found in the hydride database, check if the input is an element symbol.
        if len(hydride_name) <= 2:
            for element in self.periodic_table_of_elements.periodic_table:
                if element.get("Symbol") == hydride_name:
                    self.logger.info(
                        f"Found element {hydride_name}: Density = {element.get('Density')}"
                    )
                    return element.get("Density")
        self.logger.error(f"{normalized} is not in the database.")
        return None

    def get_bulk_conductivity(self, hydride_name: str, state: str = "Hydrogenated") -> float:
        """
        Retrieve the bulk conductivity of a hydride for a specified state.

        Parameters:
            hydride_name (str): The hydride identifier.
            state (str): The material state (e.g., "Hydrogenated").

        Returns:
            float: Bulk conductivity value or None if not found.
        """
        normalized = self._normalize_hydride_string(hydride_name)
        for entry in self.hydride_data_base:
            if entry.get("Hydride") == normalized:
                try:
                    conductivity = float(entry["Conductivity_Bulk"][state])
                    self.logger.info(
                        f"Found {normalized}: Bulk conductivity = {conductivity}"
                    )
                    return conductivity
                except (KeyError, ValueError):
                    self.logger.error(
                        f"Conductivity information for {normalized} in state '{state}' is missing or invalid."
                    )
                    return None
        self.logger.error(f"{normalized} is not in the database.")
        return None

    @staticmethod
    def _normalize_hydride_string(raw_str: str) -> str:
        """
        Normalize a hydride string to a standard format.
        For example, converts "MgH2 + 3 wt-% Fe / Mg" to "Mg1H2".

        Parameters:
            raw_str (str): The raw hydride string.

        Returns:
            str: Normalized hydride string.
        """
        match = re.search(r"^[^+/]*", raw_str)
        if match:
            hydride_name = match.group(0).strip()
            parsed = parse_chemical_formula(hydride_name)
            normalized = "".join(f"{elem}{count}" for elem, count in parsed.items())
            return normalized
        return ""

    @staticmethod
    def extract_elements(formula: str) -> dict[str, float | int]:
        """
        Extract elements and their counts from a chemical formula.

        Supports fractional stoichiometries (e.g., 0.85, 1.5) and integers.

        Parameters:
            formula (str): Chemical formula (e.g., "MgH2" or "LaNi0.85Al0.15").

        Returns:
            dict: Dictionary mapping element symbols to counts (ints or floats).
        """
        # Match element symbol + optional number (integer, decimal, or scientific notation)
        number_pattern = r"(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?"
        pattern = rf"([A-Z][a-z]*)(?:({number_pattern}))?"
        parts = re.findall(pattern, formula)

        elements: dict[str, float | int] = {}
        for elem, qty in parts:
            value = float(qty) if qty else 1.0
            # convert to int if the number is whole (e.g., 2.0 → 2)
            if value.is_integer():
                value = int(value)
            elements[elem] = elements.get(elem, 0) + value
        return elements

    def _save_database(self) -> None:
        """Save the current hydride database to the JSON file and reload it."""
        with open(self.hydride_data_base_path, "w") as file:
            json.dump(self.hydride_data_base, file, indent=4)
        self.load_hydride_database()


class PeriodicTableOfElements:
    """
    Class for managing the periodic table of elements.
    Loads element data from a JSON file.
    """

    def __init__(self, table_path: str = standard_periodic_table_path):
        """
        Initialize the PeriodicTableOfElements.

        Parameters:
            table_path (str): Path to the periodic table JSON file.
        """
        self.logger = logging.getLogger(__name__)
        self.periodic_table_path = table_path
        self.load_periodic_table()

    def load_periodic_table(self) -> None:
        """Load the periodic table from a JSON file."""
        with open(self.periodic_table_path, "r") as file:
            self.periodic_table = json.load(file)

    def atomic_mass_grabber(self, element: str) -> float:
        """
        Retrieve the atomic mass for a given element.

        Parameters:
            element (str): The element symbol (e.g., "H").

        Returns:
            float: Atomic mass in atomic mass units (u) or None if not found.
        """
        for entry in self.periodic_table:
            if entry.get("Symbol") == element:
                try:
                    atomic_mass = float(entry["AtomicMass"])
                    self.logger.debug(f"Found {element}: Mass = {atomic_mass} u")
                    return atomic_mass
                except ValueError:
                    self.logger.error(
                        f"Atomic mass of {element} is not a valid number."
                    )
                    return None
        self.logger.error(f"{element} is not recognized as a valid element.")
        return None


def create_periodic_table_of_elements() -> None:
    """
    Create a JSON file for the periodic table of elements from a CSV/text file.
    """
    input_file_path = standard_periodic_table_txt
    output_file_path = standard_periodic_table_path.replace(".txt", ".json")

    data = []
    with open(input_file_path, newline="") as file:
        reader = csv.DictReader(file)
        for row in reader:
            data.append(row)

    with open(output_file_path, "w", encoding="utf-8") as jsonfile:
        json.dump(data, jsonfile, ensure_ascii=False, indent=4)

    print(f"Data successfully written to {output_file_path}")


def create_hydride_data_base() -> None:
    """
    Create a hydride database JSON file from embedded CSV data.
    """
    csv_data = """
Hydride,Enthalpy,Entropy
Fe1H1,42300,52
Mg1H2,74701,134.7
Mg2Al3H4,62700,123
Mg2Fe1H6,79211.124243,137.188755
Mg2Ni1H4,61858,118.9
    """
    lines = csv_data.strip().split("\n")
    headers = lines[0].split(",")
    hydride_data = [dict(zip(headers, line.split(","))) for line in lines[1:]]
    with open(standard_hydride_data_base_path, "w") as file:
        json.dump(hydride_data, file, indent=4)


def test_metal_hydride_database_and_periodic_table() -> None:
    """
    Test the functionality of the MetalHydrideDatabase and PeriodicTableOfElements classes.
    """
    hydride_db = MetalHydrideDatabase()
    periodic_table = PeriodicTableOfElements()

    # Test adding a new hydride
    test_hydride = "TestHydride"
    hydride_db.hydride_adder(test_hydride, 1000, 50)

    # Test retrieving enthalpy and entropy
    enthalpy, entropy = hydride_db.get_enthalpy_entropy(test_hydride)
    print(f"Enthalpy: {enthalpy}, Entropy: {entropy}")

    # Test updating hydride information
    hydride_db.update_hydride_info(test_hydride, {"AdditionalInfo": "Test info"})

    # Test removing the hydride
    hydride_db.hydride_remover(test_hydride)

    # Test retrieving atomic mass
    atomic_mass = periodic_table.atomic_mass_grabber("H")
    print(f"Atomic Mass of H: {atomic_mass} u")

    # Test parsing chemical formula
    formula = "Mg2FeH6"
    parsed_formula = parse_chemical_formula(formula)
    print(f"{formula} parsed into {parsed_formula}")

    print("Test completed successfully.")


if __name__ == "__main__":
    # Uncomment the following line to run tests:
    # test_metal_hydride_database_and_periodic_table()

    # Example usage:
    hydride_str = "LaNi0.85Al0.15H6"
    hydride_str = "MgH2"

    mh_database = MetalHydrideDatabase()
    density = mh_database.get_density(hydride_str)
    conductivity = mh_database.get_bulk_conductivity(hydride_str)
    capacity = mh_database.get_capacity(hydride_str)

    print(f"Density: {density}")
    print(f"Bulk Conductivity: {conductivity}")
    print(f"Capacity: {capacity}")
    print(f"enthalpy, entropy {mh_database.get_enthalpy_entropy(hydride_str)}")
    #periodic_table = PeriodicTableOfElements()
    #m_Mg = periodic_table.atomic_mass_grabber("Mg")
    #m_Ni = periodic_table.atomic_mass_grabber("Ni")
    #m_H = periodic_table.atomic_mass_grabber("H")
    #print(f"M_Mg = {m_Mg}")
    #print(f"M_Ni = {m_Ni}")
    #print(f"M_H = {m_H}")
    #m_Mg2NiH4 = 2 * m_Mg + m_Ni + 4 * m_H
    #wt_Mg2NiH4 = 400 * m_H / m_Mg2NiH4

    #print (f"M_Mg2NiH4 = {m_Mg2NiH4}")
    #print(f"4*M_H/M_Mg2NiH4 *100 = {wt_Mg2NiH4} ")

    #m_MgH2 = m_Mg + 2*m_H
    #wt_MgH2 = 200*m_H/m_MgH2
    #print (f"M_MgH2 = {m_MgH2} u")
    #print(f"2*M_H/M_MgH2 *100 = {wt_MgH2} wt-%")



