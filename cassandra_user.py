#!/usr/bin/python

from cassandra.auth import PlainTextAuthProvider

try:
    from cassandra.cluster import Cluster 
    from cassandra import AuthenticationFailed
    from cassandra.cluster import NoHostAvailable
except ImportError:
    cassandra_driver_found = False
else:
    cassandra_driver_found = True


def main():
    module = AnsibleModule(
        argument_spec=dict(
            db_user=dict(default=None),
            db_password=dict(default=None),
            db_host=dict(default='localhost'),
            db_port=dict(default=9042),
            user=dict(required=True),
            password=dict(default=None),
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

    if not db_user is None and not db_password is None:
        auth_provider = PlainTextAuthProvider(username=db_user, password=db_password)
        cluster = Cluster(contact_points=[db_host], port=db_port,
                          auth_provider=auth_provider)
    else:
        cluster = Cluster([db_host], db_port)

    try:
        session = cluster.connect()

        users = session.execute('LIST USERS')

        user_found = False
        for user in users:
            if user.name == username:
                user_found = True
                if state == 'present' and update_password == 'always':
                    session.execute("ALTER USER {0} WITH PASSWORD '{1}'".format(username, password))
                    module.exit_json(changed=True, username=username, msg='Password updated')
                if state == 'absent':
                    session.execute("DROP USER IF EXISTS {0}".format(username))
                    module.exit_json(changed=True, username=username, msg='User deleted')

        if not user_found:
            session.execute("CREATE USER {0} WITH PASSWORD '{1}'".format(username, password))
            module.exit_json(changed=True, username=username, msg='User created')

        module.exit_json(changed=False, username=username)
    except Exception as error:
        module.exit_json(msg=str(error))
    finally:
        cluster.shutdown()


from ansible.module_utils.basic import *
if __name__ == '__main__':
    main()