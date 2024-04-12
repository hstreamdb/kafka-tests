#!/usr/bin/env bash
set -e

CONFIG_FILE=$PWD/local-data/config.yaml

find_freeport() {
    echo $(python3 -c '
import socketserver
with socketserver.TCPServer(("127.0.0.1", 0), None) as s:
    print(s.server_address[1])
    ')
}

generate_config() {
    env_file="local-data/dev_tools.env"
    if [ -f $env_file ]; then
        store_admin_port=$(cat $env_file | grep STORE_ADMIN_LOCAL_PORT | cut -d'=' -f2)
        zookeeper_port=$(cat $env_file | grep ZOOKEEPER_LOCAL_PORT | cut -d'=' -f2)
        base_port=$(find_freeport)
        image="hstreamdb/hstream:latest"
        sed -e "s/\${base_port}/$base_port/g" \
            -e "s#\${image}#$image#g" \
            -e "s/\${metastore_port}/$zookeeper_port/g" \
            -e "s#\${store_dir}#$PWD/local-data/logdevice#g" \
            -e "s/\${store_admin_port}/$store_admin_port/g" \
            script/config.yaml.tmpl > $CONFIG_FILE
    else
        echo "Run './script/dev-tools start-services --services store zookeeper' first!"
        exit 1
    fi
}

if [ -f $CONFIG_FILE ]; then
    echo "Config file found"
else
    echo "Config file not found, generating one..."
    generate_config
fi

CONFIG_FILE=$CONFIG_FILE ./gradlew test $@
