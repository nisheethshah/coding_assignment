import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import main
from pymongo import MongoClient

class TestETLPipeline(unittest.TestCase):

    def setUp(self):
        # Sample data for testing
        self.sample_data = pd.DataFrame({
            'FirstName': ['John', 'Jane'],
            'LastName': ['Doe', 'Doe'],
            'Company': ['Company A', 'Company B'],
            'BirthDate': ['01011980', '02021985'],
            'Salary': [60000, 75000],
            'Address': ['123 Street', '456 Avenue'],
            'Suburb': ['Suburb A', 'Suburb B'],
            'State': ['State A', 'State B'],
            'Post': ['1234', '5678'],
            'Phone': ['123-456-7890', '098-765-4321'],
            'Mobile': ['111-222-3333', '444-555-6666'],
            'Email': ['john@example.com', 'jane@example.com']
        })

    def test_read_data(self):
        # Test reading data from CSV
        with patch('pandas.read_csv') as mock_read_csv:
            mock_read_csv.return_value = self.sample_data
            data = main.read_data('member-data.csv')
            self.assertIsNotNone(data)
            self.assertEqual(list(data.columns), [
                'FirstName', 'LastName', 'Company', 'BirthDate', 'Salary',
                'Address', 'Suburb', 'State', 'Post', 'Phone', 'Mobile', 'Email'
            ])

    def test_transform_data(self):
        # Test data transformation
        transformed_data = main.transform_data(self.sample_data)
        self.assertIn('FullName', transformed_data.columns)
        self.assertIn('Age', transformed_data.columns)
        self.assertIn('SalaryBucket', transformed_data.columns)
        self.assertNotIn('FirstName', transformed_data.columns)
        self.assertNotIn('LastName', transformed_data.columns)
        self.assertEqual(transformed_data.loc[0, 'FullName'], 'John Doe')
        self.assertEqual(transformed_data.loc[1, 'Salary'], '$75,000.00')

    @patch('main.MongoClient')
    def test_load_data(self, mock_mongo_client):
        # Mock MongoDB client
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance

        # Mock the database and collection
        mock_db = MagicMock()
        mock_client_instance.__getitem__.return_value = mock_db
        mock_collection = MagicMock()
        mock_db.__getitem__.return_value = mock_collection

        # Load data into MongoDB
        main.load_data(self.sample_data, 'mongodb+srv://nisheethshah:xvrnJ8iW35FYC671@codingassignment.fmfbgrn.mongodb.net/?retryWrites=true&w=majority&appName=CodingAssignment'
                       , 'coding_assignment', 'employees')

        # Assertions
        mock_collection.drop.assert_called_once()
        mock_collection.insert_many.assert_called_once_with(self.sample_data.to_dict(orient='records'))


if __name__ == '__main__':
    unittest.main()
