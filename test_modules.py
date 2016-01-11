from subprocess import Popen, PIPE
import json
import unittest
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement, ConsistencyLevel

OUTPUT_DELIMITER = 'PARSED OUTPUT'


class CassandraKeyspaceTest(unittest.TestCase):
    session = None

    @classmethod
    def setUpClass(cls):
        auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
        cluster = Cluster(contact_points=['192.168.33.120'], auth_provider=auth_provider, protocol_version=3)
        CassandraKeyspaceTest.session = cluster.connect()

    @classmethod
    def tearDownClass(cls):
        CassandraKeyspaceTest.session.shutdown()

    def setUp(self):
        CassandraKeyspaceTest.session.execute(self.create_statement('DROP USER IF EXISTS testuser'))

    def create_statement(self, statement):
        return SimpleStatement(statement, consistency_level=ConsistencyLevel.QUORUM)

    def tearDown(self):
        self.setUp()

    def run_module(self, module_file, args=None):
        test_module_cmd = '../ansible/hacking/test-module -m %s' % module_file
        if args is not None:
            test_module_cmd += ' -a "%s"' % args

        test = Popen(test_module_cmd, stdout=PIPE, shell=True)
        (std_out, std_err) = test.communicate()
        test.wait()

        json_output = std_out.split(OUTPUT_DELIMITER)
        return json.loads(json_output[1])

    def test_should_return_error_for_missing_user_argument_when_no_user_given(self):
        output = self.run_module('cassandra_user.py')
        self.assertEqual(output['msg'], 'missing required arguments: user,password')

    def test_should_return_error_for_missing_password_argument_when_no_password_given(self):
        output = self.run_module('cassandra_user.py', 'user=testuser')
        self.assertEqual(output['msg'], 'missing required arguments: password')

    def test_should_create_user_with_password_when_new_user_given(self):
        output = self.run_module('cassandra_user.py', 'user=testuser password=test db_host=192.168.33.120')
        self.assertEqual(output['msg'], "User created")
        self.assertEqual(output['username'], "testuser")


if __name__ == '__main__':
    unittest.main()
