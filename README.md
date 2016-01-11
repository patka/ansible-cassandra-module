# ansible-cassandra-module
These are some modules to ease the setup of cassandra servers via Ansible.
The user module will help you to create, update and delete users inside Cassandra.
Be aware that Cassandra needs to be configured to use some sort of
Authorizer that is not the AllowAllAuthorizer in order to use this module.

The keyspace module will help you create and delete keyspaces as well as
updating the replication factor. The replication factor can currently
only be set for the SimpleStrategy.

# Usage
The easiest way to use those modules is to put them in a 'library' directory
in the root of your Ansible playbook. They will be automatically picked up there.

# Testing
In case you want to run the tests provided with these modules (this is still under development)
you need to have the Ansible sources checked out on your system right next to the checkout of this
project. You also need to set the contact points for the cassandra database inside the test_modules.py
Currently this is only a skeleton and will be improved soon.

# Disclaimer
Since I have no real experience as a Python developer any feedback how to
make this code more Python style is appreciated.
