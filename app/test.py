import unittest
from unittest.mock import patch, MagicMock
from db import (
    init_db, close_db, create_customer, read_customer,
    delete_customer, update_customer_by_email, update_customer_by_stripe_id,
    delete_customer_by_email, get_all_customers
)

class TestDatabaseFunctions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        init_db()

    @classmethod
    def tearDownClass(cls):
        close_db()

    def test_create_customer(self):
        # Test creating a new customer
        customer_data = create_customer("John Doe", "john@example.com")
        self.assertIsNotNone(customer_data)

        # Test creating a duplicate customer
        with self.assertRaises(Exception):
            create_customer("John Doe", "john@example.com")

    def test_read_customer(self):
        # Test reading an existing customer
        customer_data = read_customer(1)
        self.assertIsNotNone(customer_data)

        # Test reading a non-existent customer
        non_existent_customer_data = read_customer(999)
        self.assertIsNone(non_existent_customer_data)

    # Add similar test methods for other database functions...

if __name__ == '__main__':
    unittest.main()
