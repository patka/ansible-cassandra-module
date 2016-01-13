import json
import unittest
from subprocess import Popen, PIPE

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.query import SimpleStatement, ConsistencyLevel

DB_HOST = '192.168.33.120'
USER_TO_CREATE = 'testuser'
OUTPUT_DELIMITER = 'PARSED OUTPUT'


class CassandraKeyspaceTest(unittest.TestCase):
    session = None

    @classmethod
    def setUpClass(cls):
        auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
        cluster = Cluster(contact_points=[DB_HOST], auth_provider=auth_provider, protocol_version=3)
        CassandraKeyspaceTest.session = cluster.connect()

    @classmethod
    def tearDownClass(cls):
        CassandraKeyspaceTest.session.shutdown()

    def setUp(self):
        CassandraKeyspaceTest.session.execute(self.create_statement("DROP USER IF EXISTS {}".format(USER_TO_CREATE)))

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
        self.assertEqual(output['msg'], 'missing required arguments: user')

    def test_should_return_error_for_missing_password_argument_when_no_password_given(self):
        output = self.run_module('cassandra_user.py', "user={} db_host={}".format(USER_TO_CREATE, DB_HOST))
        self.assertEqual(output['msg'], 'Password is required for this operation.')

    def test_should_create_user_with_password_when_new_user_given(self):
        output = self.run_module('cassandra_user.py',
                                 "user={} password=test db_host={}".format(USER_TO_CREATE, DB_HOST))
        self.assertEqual(output['msg'], "User created")
        self.assertEqual(output['username'], USER_TO_CREATE)
        self.assertTrue(output['changed'])
        self.assertTrue(self.user_exists(USER_TO_CREATE), 'test user was not found in database')
        self.assertTrue(self.can_login(USER_TO_CREATE, 'test'), 'New user can not login to the database.')

    def test_should_update_password_when_existing_user_and_update_password_always_given(self):
        self.run_module('cassandra_user.py', "user={} password=test db_host={}".format(USER_TO_CREATE, DB_HOST))
        output = self.run_module('cassandra_user.py',
                                 "user={} password=anothertest db_host={}".format(USER_TO_CREATE, DB_HOST))
        self.assertEqual(output['msg'], "Password updated")
        self.assertTrue(output['changed'])
        self.assertTrue(self.can_login(USER_TO_CREATE, 'anothertest'), "Password seems not to be updated.")

    def test_should_update_password_when_existing_user_and_update_password_on_create_given(self):
        self.run_module('cassandra_user.py', "user={} password=test db_host={}".format(USER_TO_CREATE, DB_HOST))
        output = self.run_module('cassandra_user.py',
                                 "user={} password=anothertest db_host={} update_password=on_create".format(
                                     USER_TO_CREATE, DB_HOST))
        self.assertFalse(output['changed'])
        self.assertTrue(self.can_login(USER_TO_CREATE, 'test'), "Password seems not to be updated.")

    def test_should_drop_user_when_existing_user_and_state_absent_given(self):
        self.run_module('cassandra_user.py', "user={} password=test db_host={}".format(USER_TO_CREATE, DB_HOST))
        output = self.run_module('cassandra_user.py', "user={} db_host={} state=absent".format(
            USER_TO_CREATE, DB_HOST))
        self.assertTrue(output['changed'])
        self.assertEqual(output['msg'], 'User deleted')
        self.assertFalse(self.can_login(USER_TO_CREATE, 'test'), "User should have been deleted")

    def test_should_make_superuser_when_superuser_true_and_normal_user_given(self):
        self.run_module('cassandra_user.py', "user={} password=test db_host={}".format(USER_TO_CREATE, DB_HOST))
        self.assertFalse(self.is_superuser(USER_TO_CREATE))
        output = self.run_module('cassandra_user.py',
                                 "user={} password=test db_host={} superuser=yes update_password=on_create".format(
                                     USER_TO_CREATE, DB_HOST))
        self.assertEqual(output['msg'], 'Superuser status changed')
        self.assertTrue(self.is_superuser(USER_TO_CREATE))

    def test_should_remove_superuser_when_no_superuser_flag_and_superuser_given(self):
        self.run_module('cassandra_user.py',
                        "user={} password=test db_host={} superuser=yes".format(USER_TO_CREATE, DB_HOST))
        self.assertTrue(self.is_superuser(USER_TO_CREATE))
        output = self.run_module('cassandra_user.py',
                                 "user={} password=test db_host={} superuser=no update_password=on_create".format(
                                     USER_TO_CREATE, DB_HOST))
        self.assertEqual(output['msg'], 'Superuser status changed')
        self.assertFalse(self.is_superuser(USER_TO_CREATE))

    def is_superuser(self, username):
        users = CassandraKeyspaceTest.session.execute('LIST USERS')
        for user in users:
            if username == user.name:
                return user.super
        self.fail("User {} was expected in the database but was not found.".format(username))

    def can_login(self, username, password):
        auth_provider = PlainTextAuthProvider(username=username, password=password)
        cluster = Cluster(contact_points=[DB_HOST], auth_provider=auth_provider, protocol_version=3)
        try:
            session = cluster.connect()
        except NoHostAvailable:
            return False
        else:
            session.shutdown()
            return True

    def user_exists(self, username):
        rows = CassandraKeyspaceTest.session.execute('LIST USERS')
        for row in rows:
            if row.name == username:
                return True
        return False


if __name__ == '__main__':
    unittest.main()
