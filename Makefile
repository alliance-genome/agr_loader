index:
	docker build -t agr_loader .
	docker volume create dbstore
	docker run --publish=7474:7474 --publish=7687:7687 -v dbstore:/data -v dbstore:/logs neo4j:3.2.1