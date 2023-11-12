import unittest
import pandas as pd
from main import get_user_data, get_transaction_data


class TransactionTest(unittest.TestCase):

    def setUp(self):
        # read sample data
        self.dir = "./test_data.csv"
        self.df = pd.read_csv(self.dir)

    def test_get_user_data(self):
        # Test if the function returns the expected columns and size
        result = get_user_data(self.df)
        self.assertTrue("agentPhoneNumber" in result.columns)
        self.assertTrue("nTransactions" in result.columns)
        self.assertTrue("uuid" in result.columns)
        self.assertEqual(len(result), 2)  # ascertain uniqueness

    def test_get_transaction_data(self):
        # Test if the function returns the expected columns
        result = get_transaction_data(self.df)
        self.assertTrue("requestTimestamp" in result.columns)
        self.assertTrue("updateTimestamp" in result.columns)


if __name__ == '__main__':
    unittest.main()
