
# dmap-import
Import DMAP data into a PostgreSQL database

## Dev Setup (Mac)

* Install ASDF, Python3, Postgres
```sh
brew install asdf
brew install python3
brew install postgresql
```
* run `asdf install` to install tools via asdf
* run `poetry install` to install python dependencies
* run `cp .env.template .env` and fill out any missing environment variables
* run `docker-compose build` to build the docker images for local testing
    * run `docker-compose up dmap_local_rds` to stand up a local postgres db
        * this imnage could be used when running pytest
    * run `docker-compose up dmap_import` to run the importer application

### Outside of Docker

1. Navigate to repository directory.
2. Update `.env` variable. Source it `source .env`.
3. Run `poetry run start` to run the ingestion process.
4. Run `psql postgresql://postgres:postgres@127.0.0.1:5432/dmap_importer` to get into the database. Alternatively, after `docker-compose up`, you can:
    1. `docker exec -it dmap_local_rds bash` 
    2. `psql -U postgres -d dmap_importer`
5. Run format, type and lint checker:
    * `poetry run black .`
    * `poetry run mypy .`
    * `poetry run pylint src tests`
6. Run tests, `poetry run pytest`.

### Alembic

To create new migration:
```sh
alembic revision -m "adding a new column"
# [optional] rename generated file so as to sort migrations by name by prepending '0xx_'
```
