import os
from psycopg2 import OperationalError
from unittest import main, mock, TestCase
from actio_python_utils import database_functions as df

# either set global environment variables or set these variables
PGSERVICE = "your_service"
DB_CONNECTION_STRING = "postgres://username:password@host:port/dbname"


if "PGSERVICE" in os.environ:
    PGSERVICE = os.environ.pop("PGSERVICE")

if "DB_CONNECTION_STRING" in os.environ:
    DB_CONNECTION_STRING = os.environ.pop("DB_CONNECTION_STRING")


class TestGetDBArgs(TestCase):
    def test_service(self):
        self.assertEqual(
            df.get_db_args(service="apple")["service"], "apple"
        )

    def test_service_and_db_args(self):
        with self.assertRaises(ValueError):
            df.get_db_args(
                service=PGSERVICE, db_args={"service": PGSERVICE}
            )

    @mock.patch.dict(
        os.environ,
        {
            "DB_CONNECTION_STRING": DB_CONNECTION_STRING,
            "PGSERVICE": PGSERVICE,
        },
    )
    def test_db_connection_string(self):
        self.assertEqual(
            df.get_db_args()["dsn"], DB_CONNECTION_STRING,
        )

    @mock.patch.dict(os.environ, {"PGSERVICE": "abc"})
    def test_pgservice(self):
        self.assertEqual(df.get_db_args()["service"], "abc")

    def test_cfg(self):
        self.assertEqual(df.get_db_args()["service"], PGSERVICE)


class TestDBConnectionDBArgs(TestCase):
    def setUp(self):
        self.db = df.DBConnection(service=PGSERVICE)

    def test_db_args(self):
        self.assertEqual(self.db.db_args["service"], PGSERVICE)


class TestDBConnectionService(TestCase):
    def setUp(self):
        self.db = df.DBConnection(service=PGSERVICE)

    def test_connect(self):
        try:
            self.db.connect()
        except OperationalError:
            self.fail("Connection to DB failed!")

    def tearDown(self):
        self.db.disconnect(False)


if __name__ == "__main__":
    main()
