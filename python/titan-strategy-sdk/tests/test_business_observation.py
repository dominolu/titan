import pickle
import unittest

from titan_strategy.observation import BusinessObservationSpec, Equals, ObservationField


class TestBusinessObservation(unittest.TestCase):
    def test_public_schema_does_not_contain_private_bindings(self):
        def spec(source):
            return BusinessObservationSpec("titan.grid", 1, (
                ObservationField("anchor_ticks", "decimal", "price_tick", source,
                                 valid_when=Equals("grid.initialized", 1)),
            ))
        old, new = spec("grid.anchor_ticks"), spec("pricing.anchor_value")
        self.assertEqual(old.describe()["schema"], new.describe()["schema"])
        self.assertNotEqual(old.describe()["projection"], new.describe()["projection"])
        self.assertNotIn("source", str(old.describe()["schema"]))
        self.assertEqual(pickle.loads(pickle.dumps(old)).describe(), old.describe())

    def test_duplicate_fields_and_implicit_units_are_rejected(self):
        field = ObservationField("count", "uint64", "count", "grid.completed_pairs")
        with self.assertRaises(ValueError):
            BusinessObservationSpec("titan.grid", 1, (field, field))
        with self.assertRaises(ValueError):
            ObservationField("value", "decimal", "", "grid.anchor_ticks")
        with self.assertRaises(ValueError):
            ObservationField("phase", "enum", "state", "grid.status")


if __name__ == "__main__":
    unittest.main()
