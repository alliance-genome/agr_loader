build: pull
	docker build -t agrdocker/agr_loader_run:latest .

buildenv: build

startdb:
	docker-compose up -d neo4j

stopdb:
	docker-compose stop neo4j

pull:
	docker pull agrdocker/agr_neo4j_env:build
	docker pull agrdocker/agr_base_linux_env:build

removedb:
	docker-compose down -v

run: build
	docker-compose up agr_loader

run_test_travis:  
	build
	docker-compose run agr_loader_travis
	docker-compose run agr_loader_test_unit_tests

run_test: build
	docker-compose run agr_loader_test
	docker-compose run agr_loader_test_unit_tests

unit_tests:
	docker-compose run agr_loader_test_unit_tests

bash:
	docker-compose up agr_loader bash

# reload targets do remove and re-download files to the local docker volume.
reload: 
	docker-compose up -d neo4j
	docker-compose down -v
	docker-compose up -d neo4j
	sleep 10
	docker build -t agrdocker/agr_loader_run:latest .
	docker-compose up agr_loader

reload_test: 
	docker-compose up -d neo4j
	docker-compose down
	docker-compose up -d neo4j
	sleep 10
	docker build -t agrdocker/agr_loader_run:latest .
	docker-compose up agr_loader_test

# rebuild targets do not remove and re-download files to the local docker volume.
rebuild:
	docker-compose up -d neo4j
	docker-compose down
	docker-compose up -d neo4j
	sleep 10
	docker build -t agrdocker/agr_loader_run:latest .
	docker-compose up agr_loader

rebuild_test:
	docker-compose up -d neo4j
	docker-compose down
	docker-compose up -d neo4j
	sleep 10
	docker build -t agrdocker/agr_loader_run:latest .
	docker-compose up agr_loader_test
