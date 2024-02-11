#!/bin/sh

sed -e "s|{ENV:METASTORE_WAREHOUSE_DIR}|$METASTORE_WAREHOUSE_DIR|" \
    -e "s|{ENV:DATABASE_URL}|$DATABASE_URL|" \
    -e "s|{ENV:AZURE_SAS_TOKEN}|$AZURE_SAS_TOKEN|" \
    /opt/hive/tpl/hive-site.template.xml > /hive_conf/hive-site.xml

exec sh -c /entrypoint.sh
