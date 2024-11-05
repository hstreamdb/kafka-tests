#!/usr/bin/env bash
set -e

hstream_image=${hstream_image:-hstreamdb/hstream:latest}
server_exe=${server_exe:-"hstream-server kafka"}
CONFIG_FILE=${CONFIG_FILE:-$PWD/local-data/config.yaml}

find_freeport() {
    echo $(python3 -c '
import socketserver
with socketserver.TCPServer(("127.0.0.1", 0), None) as s:
    print(s.server_address[1])
    ')
}

ENV_FILE="local-data/dev_tools.env"

start_required_services() {
    if [ -f $ENV_FILE ]; then
        echo "Ignore starting services..."
    else
        echo "Starting required services..."
        ./script/dev-tools start-services --services store zookeeper
    fi
}

generate_config() {
    if [ -f $ENV_FILE ]; then
        echo "Generating config file..."
        store_admin_port=$(cat $ENV_FILE | grep STORE_ADMIN_LOCAL_PORT | cut -d'=' -f2)
        zookeeper_port=$(cat $ENV_FILE | grep ZOOKEEPER_LOCAL_PORT | cut -d'=' -f2)
        base_port=$(find_freeport)  # Optional
        sed -e "s/\${base_port}/$base_port/g" \
            -e "s#\${image}#$hstream_image#g" \
            -e "s#\${server_exe}#$server_exe#g" \
            -e "s/\${metastore_port}/$zookeeper_port/g" \
            -e "s#\${store_dir}#$PWD/local-data/logdevice#g" \
            -e "s/\${store_admin_port}/$store_admin_port/g" \
            script/config.yaml.tmpl > $CONFIG_FILE
    else
        echo "Error: $ENV_FILE not found. Please run 'start_required_services' first."
        exit 1
    fi
}

if [ -f $CONFIG_FILE ]; then
    echo "Config file found"
else
    echo "Config file not found, generating one..."
    start_required_services
    generate_config
fi

CONFIG_FILE=$CONFIG_FILE ./gradlew test $@
