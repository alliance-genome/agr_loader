index:
	docker stop agr_neo || true && docker rm agr_neo || true
	docker stop agr_loader || true && docker rm agr_loader || true
	docker build -t agr_loader .
	docker volume create dbstore
	docker run --publish=7474:7474 --publish=7687:7687 -v dbstore:/data -v dbstore:/logs --name agr_neo --env=NEO4J_AUTH=none neo4j:3.2.1 &
	sleep 10
	docker run --name agr_loader agr_loader 

index_quick:
	docker stop agr_loader || true && docker rm agr_loader || true
	docker build -t agr_loader .
	docker run --name agr_loader agr_loader