# syntax=docker/dockerfile:1
FROM amsterdam/gob_wheelhouse:3.9-slim-bullseye as wheelhouse
MAINTAINER datapunt@amsterdam.nl


# Application stage.
FROM amsterdam/gob_baseimage:3.9-slim-bullseye as application
MAINTAINER datapunt@amsterdam.nl

# Fill the wheelhouse.
COPY --from=wheelhouse /opt/wheelhouse /opt/wheelhouse

# Install gobbagextract in /app folder.
WORKDIR /app

# Install required Python packages.
COPY requirements.txt /app/
RUN LIBGDAL_VERSION=$(gdal-config --version) pip install --no-cache-dir \
	--find-links /opt/wheelhouse --requirement requirements.txt
RUN rm requirements.txt
# Wheelhouse cleanup.
RUN rm -rf /opt/wheelhouse

# Create directory for client certs (datapunt Openstack).
RUN mkdir -p /certs/gob/ && chmod -R 755 /certs

# Copy gobbagextract module.
COPY gobbagextract gobbagextract

# Copy Alembic config.
COPY alembic alembic
COPY alembic.ini alembic.ini

USER datapunt
CMD ["python", "-m", "gobbagextract"]


# Test.
FROM application as test
USER root
# Remove gobcore/gobconfig tests (if present).
RUN rm -rf /app/src/gobcore/tests
RUN rm -rf /app/src/gobconfig/tests

# Copy tests.
COPY tests tests
COPY .flake8 .flake8
COPY test.sh test.sh

# Copy wait for PostgreSQL script.
COPY wait-for-postgres.sh wait-for-postgres.sh
RUN chmod +x wait-for-postgres.sh

# Copy Jenkins files.
COPY .jenkins /.jenkins

USER datapunt
