#!/bin/bash

HOME=/root

#GPG v1
#echo $1 | /usr/bin/gpg --passphrase-fd 0 --no-tty --armor -u $3 --output /tmp/gooddata-sso.txt.$$ --sign $2
#echo $1 | /usr/bin/gpg --passphrase-fd 0 --no-tty --trust-model always --armor --output $2.enc --encrypt --recipient $4 /tmp/gooddata-sso.txt.$$

#GPG v2
/usr/bin/gpg --armor -u $3 --output /tmp/gooddata-sso.txt.$$ --sign $2
/usr/bin/gpg --trust-model always --armor --output $2.enc --encrypt --recipient $4 /tmp/gooddata-sso.txt.$$

chown apache:apache $2.enc
rm /tmp/gooddata-sso.txt.$$