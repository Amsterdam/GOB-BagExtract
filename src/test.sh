#!/usr/bin/env bash

set -u # crash on missing env
set -e # stop on any error

# Clear any cached results
find . -name "*.pyc" -exec rm -f {} \;

echo "Running coverage tests"
pytest tests/ --cov=gobbagextract --cov-report html --cov-report term-missing --cov-fail-under=100

echo "Running style checks"
flake8
