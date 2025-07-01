import unittest
import pandas as pd
import sys
import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from etl_pipeline.etl import transform
from etl_pipeline.transformation_functions import calculate_delivery_status

class TestTransformFunction(unittest.TestCase):

    def test_transform(self):
        logger.info("Starting test_transform.")
        # Create a sample DataFrame
        data = {
            'id': [1],
            'events': [[
                {'type': 'DELIVERY_STARTED', 'timestamp': '2022-01-01T12:00:00Z'},
                {'type': 'PACKAGE_DELIVERED', 'timestamp': '2022-01-01T12:15:00Z'}
            ]],
            'scheduled_time': ['2022-01-01T12:00:00Z']
        }
        deliveries_df = pd.DataFrame(data)
        logger.debug(f"Input DataFrame for transform:\n{deliveries_df.to_string()}")

        # Test the transform function
        transformed_df = transform(deliveries_df)
        logger.debug(f"Transformed DataFrame received:\n{transformed_df.to_string()}")


        logger.info("Performing assertions for test_transform.")
        self.assertEqual(len(transformed_df), 1, "Transformed DataFrame length mismatch.")
        self.assertIn('start_time', transformed_df.columns, "Column 'start_time' not found.")
        self.assertIn('delivered_time', transformed_df.columns, "Column 'delivered_time' not found.")
        self.assertIn('delivery_status', transformed_df.columns, "Column 'delivery_status' not found.")
        logger.info("All assertions for test_transform passed.")
        logger.info("Finished test_transform.")


class TestCalculateDeliveryStatus(unittest.TestCase):

    def test_on_time(self):
        logger.info("Starting test_on_time.")
        row = {
            'scheduled_time': datetime.datetime(2022, 1, 1, 12, 0, 0),
            'delivered_time': datetime.datetime(2022, 1, 1, 12, 10, 0)
        }
        logger.debug(f"Test row for on-time: {row}")
        result = calculate_delivery_status(row)
        self.assertEqual(result, "on-time", "Test 'on-time' failed.")
        logger.info(f"test_on_time passed. Result: {result}")

    def test_late(self):
        logger.info("Starting test_late.")
        row = {
            'scheduled_time': datetime.datetime(2022, 1, 1, 12, 0, 0),
            'delivered_time': datetime.datetime(2022, 1, 1, 12, 20, 0)
        }
        logger.debug(f"Test row for late: {row}")
        result = calculate_delivery_status(row)
        self.assertEqual(result, "late", "Test 'late' failed.")
        logger.info(f"test_late passed. Result: {result}")

    def test_missing(self):
        logger.info("Starting test_missing.")
        row = {
            'scheduled_time': datetime.datetime(2022, 1, 1, 12, 0, 0),
            'delivered_time': None
        }
        logger.debug(f"Test row for missing: {row}")
        result = calculate_delivery_status(row)
        self.assertEqual(result, "missing", "Test 'missing' failed.")
        logger.info(f"test_missing passed. Result: {result}")

def run_all_tests():

    logger.info("Collecting and running unit tests within test_etl.py...")
    
    suite = unittest.TestSuite()
    loader = unittest.TestLoader() # <--- Instantiate TestLoader
    
    # Use loadTestsFromTestCase to add tests from each class
    suite.addTest(loader.loadTestsFromTestCase(TestTransformFunction))
    suite.addTest(loader.loadTestsFromTestCase(TestCalculateDeliveryStatus))

    runner = unittest.TextTestRunner(verbosity=1)
    test_results = runner.run(suite)
    
    logger.info("Finished running unit tests within test_etl.py.")
    return test_results

if __name__ == '__main__':
    logger.info("Running test_etl.py directly.")
    results = run_all_tests()
    if results.wasSuccessful():
        logger.info("All tests passed when running test_etl.py directly.")
    else:
        logger.error("Some tests failed when running test_etl.py directly.")
        sys.exit(1)