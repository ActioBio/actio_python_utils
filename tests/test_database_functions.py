import os
from psycopg2 import OperationalError
from unittest import main, mock, TestCase
from actio_python_utils import database_functions as df


class test_get_db_args(TestCase):
    def test_service(self):
        self.assertEqual(
            df.get_db_args(service="apple")["service"], "apple"
        )

    def test_service_and_db_args(self):
        with self.assertRaises(ValueError):
            df.get_db_args(
                service="bioinfo_data_psql", db_args={"service": "bioinfo_data_psql"}
            )

    @mock.patch.dict(
        os.environ,
        {
            "DB_CONNECTION_STRING": "postgres://your_user:your_password@your_host:your_port/your_database",
            "PGSERVICE": "xyz",
        },
    )
    def test_db_connection_string(self):
        self.assertEqual(
            df.get_db_args()["dsn"],
            "postgres://your_user:your_password@your_host:your_port/your_database",
        )

    @mock.patch.dict(os.environ, {"PGSERVICE": "abc"})
    def test_pgservice(self):
        self.assertEqual(df.get_db_args()["service"], "abc")

    def test_cfg(self):
        self.assertEqual(df.get_db_args()["service"], "bioinfo_data_psql")


class test_db_connection_db_args(TestCase):
    def setUp(self):
        self.db = df.DBConnection(service="bioinfo_data_psql")

    def test_db_args(self):
        self.assertEqual(self.db.db_args["service"], "bioinfo_data_psql")


class test_db_connection_service(TestCase):
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
    main()
