#!/bin/sh

configure_client() {

    local client_config="$HOME/.tezos-client/config"
    mkdir -p "$client_dir" "$HOME/.tezos-client"

    if [ ! -f "$client_config" ]; then
        "$client" --base-dir "$client_dir" \
                  --addr "$NODE_HOST" --port "$RPC_PORT" \
                  config init --output "$client_config" >/dev/null 2>&1
    else
        "$client" --base-dir "$client_dir" \
                  --addr "$NODE_HOST" --port "$RPC_PORT" \
                  config update >/dev/null 2>&1
    fi

}

wait_for_the_node_to_be_ready() {
    local count=0
    if "$client" rpc call /blocks/head/hash >/dev/null 2>&1; then return; fi
    printf "Waiting for the node to initialize..."
    sleep 1
    while ! "$client" rpc call /blocks/head/hash >/dev/null 2>&1
    do
        count=$((count+1))
        if [ "$count" -ge 30 ]; then
            echo " timeout."
            exit 2
        fi
        printf "."
        sleep 1
    done
    echo " done."
}

wait_for_the_node_to_be_bootstraped() {
    wait_for_the_node_to_be_ready
    echo "Waiting for the node to synchronize with the network..."
    "$client" bootstrapped
}

launch_node() {

    mkdir -p "$node_dir"

    if [ ! -f "$node_dir/config.json" ]; then
        echo "Configuring the node..."
        "$node" config init \
                --data-dir "$node_dir" \
                --net-addr ":$P2P_PORT" \
                --rpc-addr ":$RPC_PORT" \
                "$@"
    else
        echo "Updating the node configuration..."
        "$node" config update \
                --data-dir "$node_dir" \
                --net-addr ":$port" \
                --rpc-addr ":$RPC_PORT" \
                "$@"
    fi

    for i in "$@"; do
        if [ "$i" = "--help" ] ; then exit 0; fi
    done

    # Check if we have to reset the chain because the image we want to
    # run has a incompatible version with the blockchain we have stored
    # locally on disk

    local image_version="$(cat "/usr/local/share/tezos/alphanet_version")"
    echo "Current public chain: $image_version."
    if [ -f "$DATA_DIR/alphanet_version" ]; then
        local local_data_version="$(cat "$DATA_DIR/alphanet_version")"
        echo "Local chain data: $local_data_version."
        if [ "$local_data_version" != "$image_version" ]; then
            echo "Removing outdated chain data..."
            if [ -f "$node_dir/identities.json" ]; then \
                mv "$node_dir/identities.json" /tmp
            fi
            rm -rf "$node_dir/*"
            ## TODO also remove stored nonces and endorsement...
            if [ -f "/tmp/identities.json" ]; then \
                mv /tmp/identities.json "$node_dir/"
            fi
            cp "/usr/local/share/tezos/alphanet_version" \
               "$DATA_DIR/alphanet_version"
        fi
    fi

    # Generate a new identity if not present

    if [ ! -f "$node_dir/identity.json" ]; then
        echo "Generating a new node identity..."
        "$node" identity generate 24. \
                --data-dir "$node_dir"
    fi

    configure_client

    # Launching the node

    exec "$node" run --data-dir "$node_dir"

}

launch_baker() {
    configure_client
    wait_for_the_node_to_be_bootstraped
    exec "$client" launch daemon --baking "$@"
}

launch_endorser() {
    configure_client
    wait_for_the_node_to_be_bootstraped
    exec "$client" launch daemon --endorsement "$@"
}
