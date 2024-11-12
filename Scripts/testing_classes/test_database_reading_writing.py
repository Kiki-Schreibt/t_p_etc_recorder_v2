import unittest
from unittest.mock import MagicMock, patch

class TestModbusProcessor(unittest.TestCase):

    def setUp(self):
        # Set up environment before each test
        self.processor = ModbusProcessor()
        self.processor.logger = MagicMock()  # Mocking the logger
        self.processor.meta_data = MagicMock()  # Assuming meta_data is needed

    @patch('database_reading_writing.DatabaseConnection')  # Mock the DatabaseConnection context manager
    def test_delete_data_from_table(self, mock_db_conn):
        # Mock cursor and connection
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_db_conn.return_value.__enter__.return_value = mock_cursor
        mock_cursor.connection = mock_conn

        # Test data
        data_to_delete_time = ['2023-01-01T00:00:00Z']

        # Run the method
        self.processor._delete_data_from_table(data_to_delete_time)

        # Assertions to check if the delete was called correctly
        mock_cursor.executemany.assert_called_once()
        args, _ = mock_cursor.executemany.call_args
        self.assertIn('DELETE FROM', args[0])  # Check if DELETE statement is in the SQL
        self.assertEqual(args[1], [(data_to_delete_time[0],)])  # Check if data is correctly formatted as tuple

        # Check if commit was called
        mock_conn.commit.assert_called_once()

        # Check for logger info calls
        self.processor.logger.info.assert_called_with("Data deleted successfully")

    def tearDown(self):
        # Clean up after each test
        pass



# Run the tests
if __name__ == '__main__':
    unittest.main()

