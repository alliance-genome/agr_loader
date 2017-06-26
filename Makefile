index:
	docker stop agr_neo || true && docker rm agr_neo || true
	docker build -t agr_loader .
	docker volume create dbstore
	docker run --publish=7474:7474 --publish=7687:7687 -v dbstore:/data -v dbstore:/logs --name agr_neo neo4j:3.2.1  &
	sleep 7
	cd src && python prototype_index.py