import json

from connections import DatabaseConnection
from table_config import TableConfig

try:
    import core.logger as logging
except ImportError:
    import logging

#todo: reimports all hydrides from .json every start of main.py. Fix to only do that when database is empty
class HydrideJsonImporter:
    """
    Imports hydride data from a JSON file into the PostgreSQL hydrides table.
    """

    def __init__(self, db_conn_params, json_path):
        self.db_conn_params = db_conn_params
        self.json_path = json_path
        self.logger = logging.getLogger(__name__)

        self.table = TableConfig().HydrideTable

    # --------------------------------------------------

    def load_json(self):
        with open(self.json_path, "r") as f:
            return json.load(f)

        # --------------------------------------------------

    def _safe_float(self, value):
        if value in ("", None, "NaN"):
            return None
        try:
            return float(value)
        except Exception:
            return None


    def normalize_entry(self, entry):

        hydride = entry.get("Hydride")
        if not hydride:
            return None

        density = entry.get("Density", {})
        conductivity = entry.get("Conductivity_Bulk", {})

        return {
            self.table.hydride: hydride.strip(),

            self.table.enthalpy: self._safe_float(entry.get("Enthalpy")),
            self.table.entropy: self._safe_float(entry.get("Entropy")),
            self.table.density_h: self._safe_float(density.get("Hydrogenated")),
            self.table.density_dh: self._safe_float(density.get("Dehydrogenated")),

            self.table.conductivity_h: self._safe_float(conductivity.get("Hydrogenated")),
            self.table.conductivity_dh: self._safe_float(conductivity.get("Dehydrogenated")),
            }
    # --------------------------------------------------

    def insert_entry(self, conn, data):
        """
        Insert a single hydride entry with conflict handling.
        """

        query = f"""
        INSERT INTO {self.table.table_name} (
            {self.table.hydride},
            {self.table.enthalpy},
            {self.table.entropy},
            {self.table.density_h},
            {self.table.density_dh},
            {self.table.conductivity_h},
            {self.table.conductivity_dh}
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT ({self.table.hydride})
        DO UPDATE SET
            {self.table.enthalpy} = EXCLUDED.{self.table.enthalpy},
            {self.table.entropy} = EXCLUDED.{self.table.entropy},
            {self.table.density_h} = EXCLUDED.{self.table.density_h},
            {self.table.density_dh} = EXCLUDED.{self.table.density_dh},
            {self.table.conductivity_h} = EXCLUDED.{self.table.conductivity_h},
            {self.table.conductivity_dh} = EXCLUDED.{self.table.conductivity_dh};
        """

        values = (
            data[self.table.hydride],
            data[self.table.enthalpy],
            data[self.table.entropy],
            data[self.table.density_h],
            data[self.table.density_dh],
            data[self.table.conductivity_h],
            data[self.table.conductivity_dh],
        )

        conn.cursor.execute(query, values)

    # --------------------------------------------------

    def run(self):
        """
        Execute full import.
        """

        data = self.load_json()

        if not data:
            self.logger.warning("No data found in JSON.")
            return

        inserted = 0

        with DatabaseConnection(**self.db_conn_params) as conn:

            for entry in data:

                normalized = self.normalize_entry(entry)

                if not normalized:
                    continue

                try:
                    self.insert_entry(conn, normalized)
                    inserted += 1

                except Exception as e:
                    self.logger.error(f"Failed to insert {entry}: {e}")

            conn.cursor.connection.commit()

        self.logger.info(f"Hydride import complete. Processed: {inserted}")



from config_reader import config
from standard_paths import standard_hydride_data_base_path

importer = HydrideJsonImporter(
    db_conn_params=config.db_conn_params,
    json_path=standard_hydride_data_base_path
)

importer.run()
