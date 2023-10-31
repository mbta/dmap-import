FROM python:3.12-slim

# Prevents buffering for easier container logging
ENV PYTHONUNBUFFERED 1

# Install non python dependencies
RUN apt-get update
RUN apt-get install -y libpq-dev curl

# Fetch Amazon RDS certificate chain
RUN curl https://s3.amazonaws.com/rds-downloads/rds-ca-2019-root.pem -o /usr/local/share/amazon-certs.pem
RUN echo "d464378fbb8b981d2b28a1deafffd0113554e6adfb34535134f411bf3c689e73 /usr/local/share/amazon-certs.pem" | sha256sum -c -
RUN chmod a=r /usr/local/share/amazon-certs.pem

# Install poetry
RUN pip install -U pip
RUN pip install "poetry==1.6.1"

WORKDIR /dmap_importer

# copy pyproject, readme, and source code into the container
COPY pyproject.toml pyproject.toml
COPY src src

# install python dependencies and build project
RUN poetry install --no-dev --no-interaction --no-ansi -v
