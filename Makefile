index:
	docker stop agr_neo || true && docker rm agr_neo || true
	docker stop agr_loader || true && docker rm agr_loader || true
	docker stop agr_env || true && docker rm agr_env || true
	docker build -t agr_loader .
	docker volume create dbstore
	docker run --publish=7474:7474 --publish=7687:7687 -v dbstore:/data -v dbstore:/logs --name agr_neo neo4j:3.2.1 &
	sleep 7
	docker run --name agr_env christabone/homeone:agr_loader_env_0.3
	docker run --name agr_loader agr_loader