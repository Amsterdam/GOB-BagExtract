#!/usr/bin/env bash

set -u # crash on missing env
set -e # stop on any error

# Clear any cached results
find . -name "*.pyc" -exec rm -f {} \;

echo "Running tests"
coverage run -m pytest tests/ --cov-fail-under=100

echo "Running coverage report"
coverage report term-missing

echo "Running style checks"
flake8
