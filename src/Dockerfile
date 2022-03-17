FROM amsterdam/python:3.9-buster as application
MAINTAINER datapunt@amsterdam.nl

ENV REQUESTS_CA_BUNDLE /etc/ssl/certs/ca-certificates.crt

RUN apt-get update && apt-get -y --no-install-recommends install unixodbc-dev libgdal-dev

# Install gobbagextract in /app folder
WORKDIR /app

# Install required Python packages
COPY requirements.txt /app/
RUN LIBGDAL_VERSION=$(gdal-config --version) pip3 install --no-cache-dir -r requirements.txt
RUN rm requirements.txt

# update custom root crt
COPY *.crt /usr/local/share/ca-certificates/extras/
RUN chmod -R 644 /usr/local/share/ca-certificates/extras/ \
 && update-ca-certificates

# Create directory for client certs
RUN mkdir -p /certs/gob/ && chmod -R 755 /certs

# Copy gobbagextract module
COPY gobbagextract gobbagextract

# Copy alembic config
COPY alembic alembic
COPY alembic.ini alembic.ini

USER datapunt

CMD ["python", "-m", "gobbagextract"]
FROM application as development
# Install GOB-Core and GOB-Config in development mode. Will be overridden with
# mounts from the developer filesystem. Changes made to GOB-Core and GOB-Config
# will be available immediately.
USER root
RUN git clone --depth=1 https://github.com/Amsterdam/GOB-Core.git
RUN LIBGDAL_VERSION=$(gdal-config --version) pip install -e /app/GOB-Core
RUN git clone --depth=1 https://github.com/Amsterdam/GOB-Config.git
RUN pip install -e /app/GOB-Config
USER datapunt

FROM application as test
# Run tests as root for now
USER root

# Remove gobcore/gobconfig tests
RUN rm -rf /app/src/gobcore/tests
RUN rm -rf /app/src/gobconfig/tests

# Copy tests
COPY tests tests
COPY .flake8 .flake8
COPY test.sh test.sh

# Copy jenkins files
COPY .jenkins /.jenkins

RUN mkdir -p /app/shared/message_broker && chown datapunt /app/shared
USER datapunt

