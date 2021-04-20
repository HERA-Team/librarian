#!/bin/bash

# put ssh config files in place
mkdir -p ~/.ssh
chmod 700 ~/.ssh
cat /secrets/id_rsa_pub > ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys

# start sshd process
echo "starting sshd process"
/usr/sbin/sshd -D -o ListenAddress=0.0.0.0 -o PasswordAuthentication=no
