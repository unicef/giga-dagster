#!/bin/bash

set -euxo pipefail

mkdir -p /hive_conf

sed -e "s|{ENV:HMS_DATABASE_URL}|$HMS_DATABASE_URL|" \
    -e "s|{ENV:HMS_POSTGRESQL_USERNAME}|$HMS_POSTGRESQL_USERNAME|" \
    -e "s|{ENV:HMS_POSTGRESQL_PASSWORD}|$HMS_POSTGRESQL_PASSWORD|" \
    /opt/hive/tpl/hive-site.template.xml > /opt/hive/conf/hive-site.xml

sed -e "s|{ENV:METASTORE_WAREHOUSE_DIR}|$METASTORE_WAREHOUSE_DIR|" \
    -e "s|{ENV:HMS_DATABASE_URL}|$HMS_DATABASE_URL|" \
    -e "s|{ENV:AZURE_SAS_TOKEN}|$AZURE_SAS_TOKEN|" \
    -e "s|{ENV:HMS_POSTGRESQL_USERNAME}|$HMS_POSTGRESQL_USERNAME|" \
    -e "s|{ENV:HMS_POSTGRESQL_PASSWORD}|$HMS_POSTGRESQL_PASSWORD|" \
    /opt/hive/tpl/metastore-site.template.xml > /opt/hive/conf/metastore-site.xml

exec sh -c /entrypoint.sh
