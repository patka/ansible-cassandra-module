#!/usr/bin/python

DOCUMENTATION = '''
---
module: cassandra_user
short_description: user management for Cassandra databases
description:
- Adds or removes users from Cassandra databases
- Sets or changes passwords for Cassandra users
- Modifies the superuser status for Cassandra users
- Be aware that cassandra requires the user management to be enabled in order to create users. You cannot login to a cassandra cluster that has AllowAllAuthorizer configured
- Therefore you should provide a db_user and db_password. If you don't provide it, the module will connect with the default credentials cassandra/cassandra.
- options:
    db_user:
        description:
            - The username used to connect to the Cassandra database
        required: False
        default: cassandra
    db_password:
        description:
            - The password used with the username to connect to the Cassandra database
        required: False
        default: cassandra
    db_host:
        description:
            - The host that should be used to connect to the Cassandra cluster. Should be one member of the cluster.
        required: False
        default: localhost
    db_port:
        description:
            - The port that will be used to connect to the cluster.
            - This is only required if Cassandra is configured to run on something else than the default port.
        required: False
        default: 9042
    protocol_version:
        description:
            - The protocol the Cassandra cluster speaks.
            - For Cassandra version 1.2 you should set 1
            - For version 2.0 take 1 or 2
            - For version 2.1 take 1,2 or 3
            - Beginning with version 2.2 you can also use 4.
        required: False
        default: 3
    user:
        description:
            - The username of the user that should be created
        required: True
    password:
        description:
            - The password that should be set for the user to create
        required: False
        default: None
    superuser:
        description:
            - If the new user is supposed to be a superuser
        required: False
        default: False
        choices: ['True', 'False', 'yes', 'no']
    state:
        description:
            - If the user should be present on the system or not. Put 'absent' if you want the user to be removed.
        required: False
        default: present
        choices: ['absent', 'present']
    update_password:
        description:
            - If the password of the user should be updated during every run or just when the user is created.
        required: False
        default: always
        choices: ['always', 'on_create']
- notes:
    - Requires cassandra-driver for python to be installed on the remote host.
    - @See U(https://datastax.github.io/python-driver) for more information on how to install this driver
    - This module should usually be configured with the 'run_once' option in Ansible since it makes no sense to create the same user from all the hosts
requirements: ['cassandra-driver']
author: "Patrick Kranz"
'''

EXAMPLES = '''
# Create a user 'testuser' with password 'testpassword' and no superuser rights:
- cassandra_user: db_user=cassandra db_password=cassandra user=testuser password=testpassword

# Create a user 'testuser' with password 'testpassword' and superuser rights:
- cassandra_user: db_user=cassandra db_password=cassandra user=testuser password=testpassword superuser=yes

# Remove an existing user 'testuser' from the database
- cassandra_user: db_user=cassandra db_password=cassandra user=testuser state=absent

'''

try:
    from cassandra import ConsistencyLevel
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.cluster import Cluster
    from cassandra.query import SimpleStatement
except ImportError:
    cassandra_driver_found = False
else:
    cassandra_driver_found = True

def superuser_string(is_superuser):
    if is_superuser:
        return "SUPERUSER"
    else:
        return "NOSUPERUSER"

def create_statement(statement):
    return SimpleStatement(statement, consistency_level=ConsistencyLevel.QUORUM)


def main():
    module = AnsibleModule(
        argument_spec=dict(
            db_user=dict(default='cassandra'),
            db_password=dict(default='cassandra'),
            db_host=dict(default='localhost'),
            db_port=dict(default=9042),
            protocol_version=dict(default=3, choices=[1,2,3,4]),
            user=dict(required=True),
            password=dict(default=None),
            superuser=dict(default='no', choices=BOOLEANS),
            state=dict(default='present', choices=['present', 'absent']),
            update_password=dict(default='always', choices=['always', 'on_create'])
        )
    )

    if not cassandra_driver_found:
        module.fail_json(msg='no cassandra driver for python found. please install cassandra-driver.')

    db_user = module.params['db_user']
    db_password = module.params['db_password']
    db_host = module.params['db_host']
    db_port = module.params['db_port']
    username = module.params['user']
    password = module.params['password']
    state = module.params['state']
    update_password = module.params['update_password']
    superuser = module.boolean(module.params['superuser'])

    auth_provider = PlainTextAuthProvider(username=db_user, password=db_password)
    cluster = Cluster(contact_points=[db_host], port=db_port,
                        auth_provider=auth_provider, protocol_version=module.params['protocol_version'])

    try:
        session = cluster.connect()

        users = session.execute('LIST USERS')

        user_found = False
        for user in users:
            if user.name == username:
                user_found = True
                if state == 'present':
                    if update_password == 'always':
                        if password is None:
                            module.fail_json(msg="Password is required in order to change password")
                        statement = create_statement('ALTER USER %s WITH PASSWORD %s')
                        session.execute(statement, (username, password))
                        module.exit_json(changed=True, username=username, msg='Password updated')
                    if user.super != superuser:
                        statement = create_statement('ALTER USER %s {0}'.format(superuser_string(superuser)))
                        session.execute(statement, [username])
                        module.exit_json(changed=True, username=username, msg="Superuser status changed")
                else:
                    statement = create_statement('DROP USER IF EXISTS %s')
                    session.execute(statement, [username])
                    module.exit_json(changed=True, username=username, msg='User deleted')

        if not user_found:
            statement = create_statement('CREATE USER %s WITH PASSWORD %s {0}'.format(superuser_string(superuser)))
            session.execute(statement, (username, password))
            module.exit_json(changed=True, username=username, msg='User created')

        module.exit_json(changed=False, username=username)
    except Exception as error:
        module.fail_json(msg=str(error))
    finally:
        cluster.shutdown()


from ansible.module_utils.basic import *
if __name__ == '__main__':
    main()
