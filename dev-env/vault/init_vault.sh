#!/bin/sh

set -e

BASE_SECRETS_DIR='/secrets'

# Check if sealed
if ! vault status; then
  echo "Vault is sealed. Unseal it..."
  SECRET_FILE=$BASE_SECRETS_DIR/secrets.txt
  if [ ! -f $SECRET_FILE ]; then
    mkdir -p $BASE_SECRETS_DIR
    vault operator init -key-shares=1 -key-threshold=1 > $SECRET_FILE
  fi
  KEY=$(awk '/^Unseal Key 1/' $SECRET_FILE | awk -F ':' '{print $2}' | xargs)
  ROOT_TOKEN=$(awk '/^Initial Root Token/' $SECRET_FILE | awk -F ':' '{print $2}' | xargs)
  vault operator unseal $KEY
fi

export VAULT_TOKEN=$ROOT_TOKEN

# Configure AppRole
if ! vault auth list | grep -q approle; then
  vault auth enable approle
fi
APP_ROLE_NAME="auth/approle/role/infinvis-readonly"
if ! vault read $APP_ROLE_NAME; then
POLICY_NAME="infinvis_readonly"
vault policy write $POLICY_NAME -<<EOF
path "secret/data/infinvis/*" {
 capabilities = [ "read" ]
}
EOF
vault write $APP_ROLE_NAME token_policies=$POLICY_NAME token_ttl=0 token_max_ttl=0 secret_id_ttl=0
ROLE_ID=$(vault read $APP_ROLE_NAME/role-id | grep 'role_id' | awk -F ' ' '{print $2}' | xargs)
SECRET_ID=$(vault write -force $APP_ROLE_NAME/secret-id | awk  '/^secret_id/ {print; exit}' | awk -F ' ' '{print $2}' | xargs)

APP_ROLE_FILE=$BASE_SECRETS_DIR/approle.txt
touch $APP_ROLE_FILE
printf "RoleId: %s\n" "$ROLE_ID" >> $APP_ROLE_FILE
printf "SecretId: %s\n" "$SECRET_ID" >> $APP_ROLE_FILE
fi

echo 'Vault has been configured'