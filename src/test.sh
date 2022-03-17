#!/usr/bin/env bash

set -u # crash on missing env
set -e # stop on any error

find . -name "*.pyc" -exec rm -f {} \;

export COVERAGE_FILE=/tmp/.coverage

echo "Running tests"
coverage run --source=./gobbagextract -m pytest tests/

echo "Running coverage report"
coverage report --show-missing --fail-under=100

echo "Running style checks"
flake8
