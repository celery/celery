#!/bin/sh
#
# Script for generating key-cert pairs
#

if [[ $# -ne 2 ]]; then
  echo "Usage: ${0##*/} KEY CERT"
  exit
fi

KEY=$1
CERT=$2
PSWD=test

read -s -p "Enter Password: " $PSWD

openssl genrsa -des3 -passout pass:$PSWD -out $KEY.key 1024
openssl req -new -key $KEY.key -out $KEY.csr -passin pass:$PSWD
cp $KEY.key $KEY.key.org
openssl rsa -in $KEY.key.org -out $KEY.key -passin pass:$PSWD
openssl x509 -req -days 365 -in $KEY.csr -signkey $KEY.key -out $CERT.crt
rm $KEY.key.org $KEY.csr
