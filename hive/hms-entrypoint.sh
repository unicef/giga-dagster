#!/bin/bash

set -euxo pipefail

sed -e "s|{ENV:HMS_DATABASE_URL}|$HMS_DATABASE_URL|" \
    -e "s|{ENV:HMS_POSTGRESQL_USERNAME}|$HMS_POSTGRESQL_USERNAME|" \
    -e "s|{ENV:HMS_POSTGRESQL_PASSWORD}|$HMS_POSTGRESQL_PASSWORD|" \
    /opt/hive/tpl/hive-site.template.xml > /opt/hive/conf/hive-site.xml

sed -e "s|{ENV:METASTORE_WAREHOUSE_DIR}|$METASTORE_WAREHOUSE_DIR|" \
    -e "s|{ENV:HMS_DATABASE_URL}|$HMS_DATABASE_URL|" \
    -e "s|{ENV:AZURE_SAS_TOKEN}|$AZURE_SAS_TOKEN|" \
    -e "s|{ENV:STORAGE_ACCOUNT_NAME}|$STORAGE_ACCOUNT_NAME|" \
    -e "s|{ENV:STORAGE_CONTAINER_NAME}|$STORAGE_CONTAINER_NAME|" \
    -e "s|{ENV:AZURE_STORAGE_ACCOUNT_KEY}|$AZURE_STORAGE_ACCOUNT_KEY|" \
    -e "s|{ENV:HMS_POSTGRESQL_USERNAME}|$HMS_POSTGRESQL_USERNAME|" \
    -e "s|{ENV:HMS_POSTGRESQL_PASSWORD}|$HMS_POSTGRESQL_PASSWORD|" \
    /opt/hive/tpl/metastore-site.template.xml > /opt/hive/conf/metastore-site.xml


SKIP_SCHEMA_INIT="${IS_RESUME:-false}"

export HIVE_CONF_DIR=$HIVE_HOME/conf

if [ -d "${HIVE_CUSTOM_CONF_DIR:-}" ]; then
  find "${HIVE_CUSTOM_CONF_DIR}" -type f -exec \
    ln -sfn {} "${HIVE_CONF_DIR}"/ \;
  export HADOOP_CONF_DIR=$HIVE_CONF_DIR
  export TEZ_CONF_DIR=$HIVE_CONF_DIR
fi

export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS:-} -Xmx1G ${SERVICE_OPTS:-}"
export METASTORE_PORT=${METASTORE_PORT:-9083}


if $HIVE_HOME/bin/schematool --verbose -dbType postgres -validate | grep 'Done with metastore validation' | grep '[SUCCESS]'; then
  echo 'Database OK'
else
  $HIVE_HOME/bin/schematool --verbose -dbType postgres -initSchema
fi

exec $HIVE_HOME/bin/hive --skiphadoopversion --skiphbasecp --service $SERVICE_NAME
