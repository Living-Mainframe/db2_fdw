#!/bin/bash
set -e

DEST=${DEST:-/host}
cd "$DEST"
export GPG_TTY=$(tty)

sed "s/PASSPHRASE/${GPG_PASSPHRASE}/" config/rpmmacros >~/.rpmmacros
echo "Importing pubkey..."
gpg --import --no-tty --batch --yes <gpg_pubkey.asc
echo "Importing seckey..."
echo "${GPG_KEY_B64}" | base64 -d | gpg --import --no-tty --batch --yes
echo "rpmsign --addsign..."
rpmsign --addsign rpms/*.rpm

echo Siging finished succesfully
