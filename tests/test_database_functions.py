import unittest
from psycopg2 import OperationalError
from actio_python_utils import database_functions as df


class test_get_db_args(unittest.TestCase):
    def test_service(self):
        self.assertEqual(
            df.get_db_args(service="bioinfo_data_psql")["service"], "bioinfo_data_psql"
        )


class test_db_connection_db_args(unittest.TestCase):
    def setUp(self):
        self.db = df.DBConnection(service="bioinfo_data_psql")

    def test_db_args(self):
        self.assertEqual(self.db.db_args["service"], "bioinfo_data_psql")


class test_db_connection(unittest.TestCase):
    def setUp(self):
        self.db = df.DBConnection(service="bioinfo_data_psql")

    def test_connect(self):
        try:
            self.db.connect()
        except OperationalError:
            self.fail("Connection to DB failed!")

    def tearDown(self):
        self.db.disconnect(False)


if __name__ == "__main__":
    unittest.main()
