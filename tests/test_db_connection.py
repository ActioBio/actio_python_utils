import unittest
from psycopg2 import OperationalError
from actio_python_utils.database_functions import DBConnection


class test_db_connection(unittest.TestCase):
    def setUp(self):
        self.db = DBConnection(service='your_service', 
                               db_args={'user': 'your_user', 
                                        'password': 'your_password', 
                                        'host': 'your_host', 
                                        'port': 'your_port', 
                                        'database': 'your_database'})
        
    def test_connect(self):
        try:
            self.db.connect()
            self.db.disconnect(False)
        except OperationalError:
            self.fail("Connection to DB failed!")

    def tearDown(self):
        self.db = None

if __name__ == "__main__":
    unittest.main()
