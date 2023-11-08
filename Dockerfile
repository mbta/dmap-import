FROM python:3.12-slim

# Prevents buffering for easier container logging
ENV PYTHONUNBUFFERED 1

# Install non python dependencies
RUN apt-get update
RUN apt-get install -y libpq-dev gcc curl gzip postgresql-client

# Fetch Amazon RDS certificate chain for Aurora Serverless v2
# https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-serverless-v2-administration.html#aurora-serverless-v2.tls
RUN curl https://www.amazontrust.com/repository/AmazonRootCA1.pem -o /usr/local/share/amazon-certs.pem
RUN chmod a=r /usr/local/share/amazon-certs.pem

# Install poetry
RUN pip install -U pip
RUN pip install "poetry==1.6.1"

# copy poetry and pyproject files and install dependencies
WORKDIR /dmap_import/
COPY pyproject.toml pyproject.toml
RUN poetry install --no-dev --no-interaction --no-ansi -v

# Copy src directory to run against and build app
COPY src src
COPY alembic.ini alembic.ini
RUN poetry install --no-dev --no-interaction --no-ansi -v
