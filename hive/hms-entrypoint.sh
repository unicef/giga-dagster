#!/bin/bash

set -euxo pipefail

sed -e "s|{ENV:HMS_DATABASE_URL}|$HMS_DATABASE_URL|" \
    -e "s|{ENV:HMS_POSTGRESQL_USERNAME}|$HMS_POSTGRESQL_USERNAME|" \
    -e "s|{ENV:HMS_POSTGRESQL_PASSWORD}|$HMS_POSTGRESQL_PASSWORD|" \
    /opt/hive-metastore/tpl/hive-site.template.xml > /opt/hadoop/etc/hadoop/hive-site.xml

sed -e "s|{ENV:METASTORE_WAREHOUSE_DIR}|$METASTORE_WAREHOUSE_DIR|" \
    -e "s|{ENV:HMS_DATABASE_URL}|$HMS_DATABASE_URL|" \
    -e "s|{ENV:AZURE_SAS_TOKEN}|$AZURE_SAS_TOKEN|" \
    -e "s|{ENV:HMS_POSTGRESQL_USERNAME}|$HMS_POSTGRESQL_USERNAME|" \
    -e "s|{ENV:HMS_POSTGRESQL_PASSWORD}|$HMS_POSTGRESQL_PASSWORD|" \
    /opt/hive-metastore/tpl/metastore-site.template.xml > /opt/hive-metastore/conf/metastore-site.xml

if /opt/hive-metastore/bin/schematool -dbType "$DATABASE_TYPE" -validate | grep 'Done with metastore validation' | grep '[SUCCESS]'; then
  echo 'Database OK'
else
  /opt/hive-metastore/bin/schematool --verbose -dbType "$DATABASE_TYPE" -initSchema
fi

exec /opt/hive-metastore/bin/start-metastore
