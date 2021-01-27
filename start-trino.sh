#!/bin/bash

export TRINO_PORT=${TRINO_PORT:-8080}
docker-compose up
