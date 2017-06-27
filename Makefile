build:
	docker build -t agrdocker/agr_loader_run .

startdb:
	docker-compose up -d neo4j_nqa

run:
	docker-compose up agr_loader

bash:
	docker-compose up agr_loader bash

