#!/usr/bin/env bash

docker run --rm --name=mssql \
  -e 'ACCEPT_EULA=Y' \
  -e 'SA_PASSWORD=1234abc7643Z' \
  -p 1433:1433 \
  mcr.microsoft.com/mssql/server:2017-CU8-ubuntu
