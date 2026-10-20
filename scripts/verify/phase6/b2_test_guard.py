import sys
import unittest
from src.ml.guard import validate_feature_selection

class TestFeatureGuard(unittest.TestCase):
    def test_valid_features(self):
        valid = ["AIRLINE", "ORIGIN_AIRPORT", "DESTINATION_AIRPORT", "DISTANCE", "SCHEDULED_TIME"]
        self.assertTrue(validate_feature_selection(valid))
        print("PASS: Valid pre-departure features accepted.")

    def test_leakage_cause_data(self):
        with self.assertRaises(ValueError) as ctx:
            validate_feature_selection(["HAS_CAUSE_DATA"])
        self.assertIn("HAS_CAUSE_DATA", str(ctx.exception))
        self.assertIn("excluded-leakage", str(ctx.exception))
        print(f"PASS: Caught forbidden feature HAS_CAUSE_DATA -> {ctx.exception}")

    def test_leakage_departure_delay(self):
        with self.assertRaises(ValueError) as ctx:
            validate_feature_selection(["DEPARTURE_DELAY"])
        self.assertIn("DEPARTURE_DELAY", str(ctx.exception))
        self.assertIn("excluded-leakage", str(ctx.exception))
        print(f"PASS: Caught forbidden feature DEPARTURE_DELAY -> {ctx.exception}")

    def test_time_split_month(self):
        with self.assertRaises(ValueError) as ctx:
            validate_feature_selection(["MONTH"])
        self.assertIn("MONTH", str(ctx.exception))
        self.assertIn("excluded-time-split", str(ctx.exception))
        print(f"PASS: Caught forbidden feature MONTH -> {ctx.exception}")

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestFeatureGuard)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
