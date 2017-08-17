build: pull
	docker build -t agrdocker/agr_loader_run:develop .

startdb:
	docker-compose up -d neo4j_nqc

stopdb:
	docker-compose stop neo4j_nqc

pull:
	docker pull agrdocker/agr_loader_env:develop

removedb:
	docker-compose down -v

run: build
	docker-compose up agr_loader

run_test: build
	docker-compose up agr_loader_test

test: removedb startdb build run_test

bash:
	docker-compose up agr_loader bash

reload:
	docker-compose stop neo4j_nqc
	docker-compose down -v
	docker-compose up -d neo4j_nqc
	sleep 10
	docker-compose up agr_loader

