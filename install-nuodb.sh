#!/bin/sh -e

wget http://www.nuodb.com/latest/nuodb-1.0-GA.linux.x86_64.deb --output-document=/var/tmp/nuodb.deb
sudo dpkg -i /var/tmp/nuodb.deb