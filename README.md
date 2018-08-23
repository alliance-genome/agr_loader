[![Build Status](https://travis-ci.org/alliance-genome/agr_loader.svg?branch=develop)](https://travis-ci.org/alliance-genome/agr_loader)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/5259a0e847c04c72a4a9c4f34fabfed5)](https://www.codacy.com/project/christabone/agr_loader/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=alliance-genome/agr_loader&amp;utm_campaign=Badge_Grade_Dashboard)


# Alliance of Genome Resources Loader
An initial loader prototype for the web portal of the Alliance of Genome
Resources.

## Requirements
- Docker
- Docker-compose

## Installation
- Build the local environment image with `make buildenv`.
- Build the local image with `make build`.
- This repository has the agr_schemas as a submodule. Do the following the first time at checkout:
  - cd into schemas directory 
  - git submodule init
  - git submodule update (to fetch the actual data in this directory)
- Start the Neo4j database with `make startdb`. Allow ~10 seconds for Neo4j to initialize.
  - The Docker volume `agrloader_storedb` will be created (if it does not already exist).
  - To initialize an empty database after previously using the loader, be sure to run `make removedb` **before** running `make startdb`.

- git submodule update --init (on subsequent updates, run: git submodule update --remote` before merging changes to the `agr_loader` repo) 
  - currently, this submodule is tracking the 1.0.4 release - we need to change .gitmodules to reflect the next version when appropriate.

- ensure that your local docker installation has access to at least 5G (preferentially 8G) of memory or else your run_test target will fail with a non-inituative error that "Cannot resolve address 'neo4j.nqc'" this can be done in the docker preferences.   

## Running the Loader
- Initialize a full load with `make run`.
- Alternatively, `make run_test` will launch a much smaller test load; this is useful for development and testing.

## Running Unit Tests
- Once the loader has been run (either test load or full load), unit tests can be executed via `make unit_tests`.

## Accessing the Neo4j Shell
- From your command line: `docker exec -ti neo4j.nqc bin/cypher-shell`
  - A quick command to count the number of nodes in your db: `match (n) return count (n);`

## Stopping and Removing the Database
- Once finished, stop the database with `make stopdb`.
- Optionally, remove the database with `make removedb`.

## Shortcut Commands
- `make reload` will re-run the `Installation` and `Running the Loader` steps from above.
- `make reload_test` will re-run the same steps using a test subset of data.
