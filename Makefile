build:
	docker build -t agrdocker/agr_loader_run .

startdb:
	docker-compose up -d neo4j_nqa

stopdb:
	docker-compose stop neo4j_nqa

removedb:
	docker-compose down -v

run:
	docker-compose up agr_loader

bash:
	docker-compose up agr_loader bash

