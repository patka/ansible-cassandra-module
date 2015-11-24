# ansible-cassandra-module
These are some modules to ease the setup of cassandra servers via Ansible.
The user module will help you to create, update and delete users inside Cassandra.
Be aware that Cassandra needs to be configured to use some sort of
Authorizer that is not the AllowAllAuthorizer in order to use this module.

The keyspace module will help you create and delete keyspaces as well as
updating the replication factor. The replication factor can currently
only be set for the SimpleStrategy.

Since I have no real experience as a Python developer any feedback how to
make this code more Python style is appreachiated.
