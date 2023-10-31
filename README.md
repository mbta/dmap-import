# dmap-import
Import DMAP data into a PostgreSQL database

## Dev Setup
* Install ASDF ([installation guide](https://asdf-vm.com/guide/getting-started.html))
* run `asdf install` to install tools via asdf
* run `poetry install` to install python dependencies
* run `cp .env.template .env` and fill out any missing environment variables
* run `docker-compose build` to build the docker images for local testing
    * run `docker-compose up dmap_local_rds` to stand up a local postgres db
        * this imnage could be used when running pytest
    * run `docker-compose up dmap_importer` to run the importer application
