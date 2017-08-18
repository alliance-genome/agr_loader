build: pull
	docker build -t agrdocker/agr_loader_run:develop .

startdb:
	docker-compose up -d neo4j.nqc

stopdb:
	docker-compose stop neo4j.nqc

pull:
	docker pull agrdocker/agr_loader_env:develop

removedb:
	docker-compose down -v

run: build
	docker-compose up agr_loader

run_test: build
	docker-compose up agr_loader_test

bash:
	docker-compose up agr_loader bash

reload: stopdb removedb startdb
	sleep 10
	make build run

reload_test: stopdb removedb startdb 
	sleep 10
	make build run_test