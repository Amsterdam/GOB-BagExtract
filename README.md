# GOB-BagExtract

GOB BAG extracts data van de BAG registratie en prepareert deze voor de import door [GOB-Import](https://github.com/Amsterdam/GOB-Import).

# Environment variables
Example environment variables are set in `.env.example`. Create your own `.env` based on this example file:

```bash
cp .env.example .env
```
To initialise the configuration:

```bash
export $(cat .env | xargs)
```

# Infrastructure

A running [GOB infrastructure](https://github.com/Amsterdam/GOB-Infra) is required to run this component.

# Docker

## Requirements

* docker compose >= 1.25
* Docker CE >= 18.09

## Run

```bash
docker compose build
docker compose up &
```

## Tests

```bash
docker compose -f src/.jenkins/test/docker-compose.yml build
docker compose -f src/.jenkins/test/docker-compose.yml run --rm test
```

# Local

## Requirements

* Python >= 3.9

## Initialisation

Create a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r src/requirements.txt
```

Or activate the previously created virtual environment:

```bash
source venv/bin/activate
```

# Run

Start the service:

```bash
cd src
python -m gobbagextract
```

## Tests

Run the tests:

```bash
cd src
sh test.sh
```

BagExtract are triggered by the [GOB-Workflow](https://github.com/Amsterdam/GOB-Workflow) module. See the GOB-Workflow README for more details.
