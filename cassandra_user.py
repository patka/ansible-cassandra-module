#!/usr/bin/python

from cassandra.auth import PlainTextAuthProvider

try:
    from cassandra import ConsistencyLevel
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

def main():
    module = AnsibleModule(
        argument_spec=dict(
            db_user=dict(required=True),
            db_password=dict(required=True),
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
                    if update_password == 'always' or user.super != superuser:
                        statement = SimpleStatement("ALTER USER %s WITH PASSWORD %s {0}".format(superuser_string(superuser)), consistency_level=ConsistencyLevel.QUORUM)
                        session.execute(statement, (username, password))
                        module.exit_json(changed=True, username=username, msg='Password and/or superuser status updated')
                else:
                    statement = SimpleStatement("DROP IF EXISTS %s", consistency_level=ConsistencyLevel.QUORUM)
                    session.execute(statement, username)
                    module.exit_json(changed=True, username=username, msg='User deleted')

        if not user_found:
            statement = SimpleStatement("CREATE USER %s WITH PASSWORD %s {0}".format(superuser_string(superuser)), consistency_level=ConsistencyLevel.QUORUM)
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