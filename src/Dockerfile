FROM amsterdam/python:3.9-buster
MAINTAINER datapunt@amsterdam.nl

RUN apt-get update && apt-get -y --no-install-recommends install unixodbc-dev libgdal-dev

# Install gobbagextract in /app folder
WORKDIR /app

# Install required Python packages
COPY requirements.txt /app/
RUN pip3 install --no-cache-dir -r requirements.txt
RUN rm requirements.txt

# Copy gobbagextract module
COPY gobbagextract gobbagextract

# Copy alembic config
COPY alembic alembic
COPY alembic.ini alembic.ini

CMD ["python", "-m", "gobbagextract"]
