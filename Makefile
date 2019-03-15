build: pull
	docker build -t agrdocker/agr_loader_run:latest .

buildenv: build

startdb:
	docker-compose up -d neo4j.nqc

stopdb:
	docker-compose stop neo4j.nqc

pull:
	docker pull agrdocker/agr_neo4j_env:latest
	docker pull agrdocker/agr_python_env:latest

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

reload: 
	docker-compose up -d neo4j.nqc
	docker-compose down -v
	docker-compose up -d neo4j.nqc
	sleep 10
	docker build -t agrdocker/agr_loader_run:latest .
	docker-compose up agr_loader

reload_test: 
	docker-compose up -d neo4j.nqc
	docker-compose down -v
	docker-compose up -d neo4j.nqc
	sleep 10
	docker build -t agrdocker/agr_loader_run:latest .
	docker-compose up agr_loader_test
