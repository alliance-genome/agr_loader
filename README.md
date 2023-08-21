[![Build Status](https://travis-ci.org/alliance-genome/agr_loader.svg?branch=develop)](https://travis-ci.org/alliance-genome/agr_loader)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/5259a0e847c04c72a4a9c4f34fabfed5)](https://www.codacy.com/project/christabone/agr_loader/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=alliance-genome/agr_loader&amp;utm_campaign=Badge_Grade_Dashboard)

#  Alliance of Genome Resources Loader
ETL pipeline for Alliance of Genome Resources

## Requirements
- Docker 
- Docker-compose
- AWS access keys (project is agr_aws).  Please contact Stuart Myasato or Olin Blodgett for permission on the AWS project.  (below are instructions for getting a login token and pulling the base images after you have access keys).

## Installation
- Build the local image with `make build`.
- Start the Neo4j database with `make startdb`. Allow ~10 seconds for Neo4j to initialize.
  - To initialize an empty database after previously using the loader, be sure to run `make removedb` **before** running `make startdb`.

- ensure that your local docker installation has access to at least 5G (preferentially 8G) of memory or else your run_test target will fail with a non-inituative error that "Cannot resolve address 'neo4j'" this can be done in the docker preferences.

## Running the Loader
- Initialize a full load with `make run`.
- Alternatively, `make run_test` will launch a much smaller test load; this is useful for development and testing.

## Running Unit Tests
- Once the loader has been run (either test load or full load), unit tests can be executed via `make unit_tests`.

## Accessing the Neo4j Shell
- From your command line: `docker exec -ti neo4j bin/cypher-shell`
  - A quick command to count the number of nodes in your db: `match (n) return count (n);`

## Stopping and Removing the Database
- Remove the database with `make removedb`.

## Shortcut Commands
- `make reload` will re-run the `Installation` and `Running the Loader` steps from above.
- `make reload_test` will re-run the same steps using a test subset of data.
- note: reload_test will not re-download the file bolus. 

## Config
- There are 3 loader configuration files that come with the system (in [`src/config`](./src/config)). Each is set up to work on a particular environment (and differs in the default number of threads for both downloading files and the number of threads used to load the database):
  - [test.yml](./src/config/test.yml) will be used while running the load using the test data set.
  - [default.yml](./src/config/default.yml) is the configuration used on all the shared systems and on production.
  - [develop.yml](./src/config/develop.yml) is used for the full data set on a development system.  
  Each can be modified to remove or add the data types (ie: Allele, BGI, Expression, etc...) and subtypes (ie: ZFIN, SGD, RGD, etc...) as needed for development purposes.
- When adding a new data load, be sure to add to [validation.yml](./src/config/validation.yml) as well so the system knows the expected data types and subtypes.
- [local_submission_system.json](./src/config/local_submission_system.json) is a file consumed in addition to the submission system data (from the submission system API) that is used to customize non-submission system files like ontology files.

## ENV Variables
- ALLIANCE_RELEASE - the release version that this code acts on.
- FMS_API_URL - the host from which this code pulls its available file paths from (submission system host).  Note: the submission system host is reliant on the ferret file grabber.  That pipeline is responsible for ontologie files and GAF files being up to date.  And, the submission system requires a snapshot to be taken to fetch 'latest' files.  
- TEST_SCHEMA_BRANCH - If set that branch of the agr_schema wil be used instead of master
- If the site is built with docker-compose, these will be set automatically to the 'dev' versions of all these variables.

## Accessing AWS (ECR) stored docker images
AWS ECR uses an token-based authentication system, for which tokens automatically expire after 12 hours. This means frequent authentication is required to access base linux and neo4j env image. To enable this:
- optionally install AWS-CLI locally. Alternatively you can also use the amazon-provided docker image (this is used in the makefile).
  To use the docker images for all steps below, replace the `aws` part of all commands with `docker run --rm -it -v ~/.aws:/root/.aws amazon/aws-cli`.
- make sure you have AWS login credentials for the agr_aws account, with the permission group - AWS group for ECR access.
- Upon setup, run `aws configure` (which will generate or append/update `~/.aws/config` and `~/.aws/credentials`) and provide the following details when asked for:
  * AWS Access Key ID: provide your personal access key ID
  * AWS Secret Access Key: provide your personal Secret Access Key (only accessible on access key creation, you may need to regenerate a new access key if you did not store it).
  * Default region name: `us-east-1` 
  * Default output format: `<enter>` (accept default)
-  To test that your credentials are working correctly, run `aws ecr get-login-password` and verify a token is returned.

The Makefile includes all required recipes and dependencies to automatically perform AWS authentification when required
for any `make` target, provided that the above aws cli configurations are in place.
Alternatively, to manually renew the authentication token stored in `.docker/config.json` (for custom debugging and development),
you can execute the following `make` command (export or pass the `AWS_PROFILE` environment variable value as your aws-profile name if you use a named aws profile for agr):
```bash
make registry-docker-login
```

_Reminder_: authentification needs to be renewed every time you get an error like this (usually ~ every 12 hours):
```
Error response from daemon: pull access denied for 100225593120.dkr.ecr.us-east-1.amazonaws.com/agr_neo4j_env, repository does not exist or may require 'docker login': denied: Your authorization token has expired. Reauthenticate and try again.
```
