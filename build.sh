#!/bin/bash
set -eux

g++ -larrow -larrow_dataset -lparquet -lduckdb arrow_sql.cc -o arrow_sql
