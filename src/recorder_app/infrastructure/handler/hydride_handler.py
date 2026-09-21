#hydride_handler.py


"""
Database-backed module for managing a metal hydride database
and the periodic table of elements.
"""

import re
import csv
import json

from recorder_app.infrastructure.connections.connections import DatabaseConnection
from recorder_app.infrastructure.core.table_config import TableConfig
from recorder_app.infrastructure.utils.standard_paths import (
    standard_periodic_table_path,
    standard_periodic_table_txt,
)

# Logging
try:
    import recorder_app.infrastructure.core.logger as custom_logging
    logging = custom_logging
except ImportError:
    import logging


# --------------------------------------------------
# Utility
# --------------------------------------------------

def parse_chemical_formula(formula: str) -> dict[str, float | int]:
    number = r"(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?"
    pattern = rf"([A-Z][a-z]*)(?:({number}))?"
    parts = re.findall(pattern, formula)

    counts: dict[str, float | int] = {}
    for element, qty in parts:
        value = float(qty) if qty else 1.0
        if value.is_integer():
            value = int(value)
        counts[element] = counts.get(element, 0) + value
    return counts


# --------------------------------------------------
# Hydride Database (DB-backed)
# --------------------------------------------------

class MetalHydrideDatabase:

    def __init__(self, db_conn_params):
        self.logger = logging.getLogger(__name__)
        self.db_params = db_conn_params
        self.table = TableConfig().HydrideTable
        self.periodic_table_of_elements = PeriodicTableOfElements()

    # --------------------------------------------------

    def get_enthalpy_entropy(self, hydride_name: str) -> tuple:
        normalized = self._normalize_hydride_string(hydride_name)

        query = f"""
            SELECT {self.table.enthalpy}, {self.table.entropy}
            FROM {self.table.table_name}
            WHERE {self.table.hydride} = %s
        """

        with DatabaseConnection(**self.db_params) as conn:
            conn.cursor.execute(query, (normalized,))
            row = conn.cursor.fetchone()

        if row:
            return float(row[0]), float(row[1])

        self.logger.error(f"{normalized} not found.")
        return None, None

    # --------------------------------------------------

    def hydride_adder(self, hydride_name: str, enthalpy: float, entropy: float) -> None:

        enthalpy *= 1e3
        entropy *= 1e3
        hydride_name = self._normalize_hydride_string(hydride_name)
        query = f"""
            INSERT INTO {self.table.table_name} (
                {self.table.hydride},
                {self.table.enthalpy},
                {self.table.entropy}
            )
            VALUES (%s, %s, %s)
            ON CONFLICT ({self.table.hydride}) DO NOTHING
        """

        with DatabaseConnection(**self.db_params) as conn:
            conn.cursor.execute(query, (hydride_name, enthalpy, entropy))
            conn.cursor.connection.commit()

    # --------------------------------------------------

    def hydride_remover(self, hydride_name: str) -> None:

        hydride_name = self._normalize_hydride_string(hydride_name)
        query = f"""
            DELETE FROM {self.table.table_name}
            WHERE {self.table.hydride} = %s
        """

        with DatabaseConnection(**self.db_params) as conn:
            conn.cursor.execute(query, (hydride_name,))
            conn.cursor.connection.commit()

    # --------------------------------------------------

    def update_hydride_info(self, hydride_name: str, new_info: dict) -> None:

        if not new_info:
            return

        set_clause = ", ".join([f"{k} = %s" for k in new_info.keys()])
        values = list(new_info.values())
        values.append(hydride_name)

        query = f"""
            UPDATE {self.table.table_name}
            SET {set_clause}
            WHERE {self.table.hydride} = %s
        """

        with DatabaseConnection(**self.db_params) as conn:
            conn.cursor.execute(query, values)
            conn.cursor.connection.commit()

    # --------------------------------------------------

    def get_density(self, hydride_name: str, state: str = "Dehydrogenated") -> float:

        normalized = self._normalize_hydride_string(hydride_name)

        column = (
            self.table.density_h
            if state == "Hydrogenated"
            else self.table.density_dh
        )

        query = f"""
            SELECT {column}
            FROM {self.table.table_name}
            WHERE {self.table.hydride} = %s
        """

        with DatabaseConnection(**self.db_params) as conn:
            conn.cursor.execute(query, (normalized,))
            row = conn.cursor.fetchone()

        if row and row[0] is not None:
            return float(row[0])

        # fallback: element lookup
        if len(hydride_name) <= 2:
            return self.periodic_table_of_elements.get_density(hydride_name)

        return None

    # --------------------------------------------------

    def get_bulk_conductivity(self, hydride_name: str, state: str = "Hydrogenated") -> float:

        normalized = self._normalize_hydride_string(hydride_name)

        column = (
            self.table.conductivity_h
            if state == "Hydrogenated"
            else self.table.conductivity_dh
        )

        query = f"""
            SELECT {column}
            FROM {self.table.table_name}
            WHERE {self.table.hydride} = %s
        """

        with DatabaseConnection(**self.db_params) as conn:
            conn.cursor.execute(query, (normalized,))
            row = conn.cursor.fetchone()

        if row and row[0] is not None:
            return float(row[0])

        return None

    # --------------------------------------------------

    def get_capacity(self, hydride_name: str) -> float:

        total_mass, hydrogen_mass = self.get_molar_mass_hydride(
            hydride_name, return_hydrogen_mass=True
        )

        if total_mass and hydrogen_mass:
            return (hydrogen_mass / total_mass) * 100

        return None

    # --------------------------------------------------

    def get_molar_mass_hydride(self, hydride_name: str, return_hydrogen_mass=False):

        normalized = self._normalize_hydride_string(hydride_name)
        element_counts = self.extract_elements(normalized)

        total_mass = 0.0
        hydrogen_mass = 0.0

        for element, count in element_counts.items():
            atomic_mass = self.periodic_table_of_elements.atomic_mass_grabber(element)
            if atomic_mass:
                total_mass += atomic_mass * count
                if element == "H":
                    hydrogen_mass += atomic_mass * count

        if return_hydrogen_mass:
            return total_mass, hydrogen_mass

        return total_mass

    # --------------------------------------------------

    @staticmethod
    def _normalize_hydride_string(raw_str: str) -> str:
        match = re.search(r"^[^+/]*", raw_str)
        if match:
            hydride_name = match.group(0).strip()
            parsed = parse_chemical_formula(hydride_name)
            return "".join(f"{e}{c}" for e, c in parsed.items())
        return ""

    # --------------------------------------------------

    @staticmethod
    def extract_elements(formula: str) -> dict[str, float | int]:
        return parse_chemical_formula(formula)


    def get_all_hydrides(self) -> list[str]:
        """
        Return a list of all hydride names in the database.
        """
        try:
            # If using a dict-like in-memory database
            if hasattr(self, "hydrides") and isinstance(self.hydrides, dict):
                return list(self.hydrides.keys())

            # If using an SQL database
            with DatabaseConnection(**self.db_params) as db_conn:

                db_conn.cursor.execute(f"SELECT {self.table.hydride} FROM {self.table.table_name}")  # Replace table/column name
                rows = db_conn.cursor.fetchall()
                return [row[0] for row in rows]



            # Fallback
            raise RuntimeError("No recognized database storage found.")

        except Exception as e:
            print(f"Error fetching hydrides: {e}")
            return []

# --------------------------------------------------
# Periodic Table
# --------------------------------------------------

class PeriodicTableOfElements:

    def __init__(self, table_path: str = standard_periodic_table_path):
        self.logger = logging.getLogger(__name__)
        self.periodic_table_path = table_path
        self.load_periodic_table()

    def load_periodic_table(self):
        with open(self.periodic_table_path, "r") as file:
            self.periodic_table = json.load(file)

    def atomic_mass_grabber(self, element: str) -> float:
        for entry in self.periodic_table:
            if entry.get("Symbol") == element:
                try:
                    return float(entry["AtomicMass"])
                except Exception:
                    return None
        return None

    def get_density(self, element: str):
        for entry in self.periodic_table:
            if entry.get("Symbol") == element:
                return entry.get("Density")
        return None


# --------------------------------------------------
# Utilities (unchanged)
# --------------------------------------------------

def create_periodic_table_of_elements() -> None:

    input_file_path = standard_periodic_table_txt
    output_file_path = standard_periodic_table_path.replace(".txt", ".json")

    data = []
    with open(input_file_path, newline="") as file:
        reader = csv.DictReader(file)
        for row in reader:
            data.append(row)

    with open(output_file_path, "w", encoding="utf-8") as jsonfile:
        json.dump(data, jsonfile, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    # Uncomment the following line to run tests:
    # test_metal_hydride_database_and_periodic_table()

    # Example usage:
    hydride_str = "LaNi4.85Al0.15H6"
    #hydride_str = "MgH2"

    mh_database = MetalHydrideDatabase(db_conn_params=config_reader.config.db_conn_params)
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



