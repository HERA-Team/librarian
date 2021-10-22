#!/bin/bash

# put ssh config files in place
HOME="${VARIABLE:=/home/sobs}"
mkdir -p $HOME/.ssh
chmod 700 $HOME/.ssh
cat /secrets/id_rsa_pub > $HOME/.ssh/authorized_keys
chmod 600 $HOME/.ssh/authorized_keys

# start sshd process
echo "starting sshd process"
/usr/sbin/sshd -D -p 2222 -o ListenAddress=0.0.0.0 -o PasswordAuthentication=no
