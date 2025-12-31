#!/bin/bash
set -e

DEST=${DEST:-/host}
cd "$DEST"
export GPG_TTY=$(tty)

sed "s/GPG_PASSPHRASE/${GPG_PASSPHRASE}/" config/signmacros >~/.rpmmacros
gpg --import --no-tty --batch --yes <RPM-GPG-KEY-myrepo
echo "Importing seckey..."
echo "${GPG_KEY_B64}" | base64 -d | gpg --import --no-tty --batch --yes
echo "rpmsign --addsign..."
rpmsign --addsign rpms/*.rpm

echo Siging finished succesfully
