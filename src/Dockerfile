FROM amsterdam/python:3.9-buster
MAINTAINER datapunt@amsterdam.nl

ENV REQUESTS_CA_BUNDLE /etc/ssl/certs/ca-certificates.crt

RUN apt-get update && apt-get -y --no-install-recommends install unixodbc-dev libgdal-dev

# Install gobbagextract in /app folder
WORKDIR /app

# Install required Python packages
COPY requirements.txt /app/
RUN LIBGDAL_VERSION=$(gdal-config --version) pip3 install --no-cache-dir -r requirements.txt
RUN rm requirements.txt

# Create directory for client certs
RUN mkdir -p /certs/gob/ && chmod -R 755 /certs

# Copy gobbagextract module
COPY gobbagextract gobbagextract

# Copy alembic config
COPY alembic alembic
COPY alembic.ini alembic.ini

USER datapunt

CMD ["python", "-m", "gobbagextract"]
