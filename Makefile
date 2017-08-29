build: pull
	docker build -t agrdocker/agr_loader_run:develop .

buildenv:
	docker build -f Dockerfile_env -t agrdocker/agr_loader_env:develop .

startdb:
	docker-compose up -d neo4j.nqc

stopdb:
	docker-compose stop neo4j.nqc

pull:
	docker pull agrdocker/agr_neo4j_env:develop

removedb:
	docker-compose down -v

run: build
	docker-compose up agr_loader

run_test: build
	docker-compose up agr_loader_test

run_unit_tests: build
	docker-compose up agr_loader_unit_tests

bash:
	docker-compose up agr_loader bash

reload: 
	docker-compose up -d neo4j.nqc
	docker-compose down -v
	docker-compose up -d neo4j.nqc
	sleep 10
	docker build -t agrdocker/agr_loader_run:develop .
	docker-compose up agr_loader

reload_test: 
	docker-compose up -d neo4j.nqc
	docker-compose down -v
	docker-compose up -d neo4j.nqc
	sleep 10
	docker build -t agrdocker/agr_loader_run:develop .
	docker-compose up agr_loader_test
