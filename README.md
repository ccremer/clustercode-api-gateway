# clustercode-api-gateway
API gateway for clustercode [WIP]

## Building

    sudo snap install go --classic # or whatever package manager you use
    sudo apt-get install g++ libxml2-dev
    go mod vendor
    go build

## Running

    # Run
    go build main.go
    ./clustercode-api-gateway
    # OR
    go run main.go

## Configuring for local development

- Copy `defaults.yaml` to `config.yaml`
- Change the values you would like to have differing from default.
- Delete other values in `config.yaml` that are already default, leaving the custom ones left.

## Testing

Unit tests:

    go test ./...

Integration tests:

    docker network create clustercode
    # incomplete yet

## Concept

This microservice takes request from the frontend and transforms these
into Rabbitmq RPC messages.

In addition, it serves as the webserver for messaging schema definition.
