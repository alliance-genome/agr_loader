# get the Elasticsearch URI from an environment variable, if one is set
test:
	$(OPTIONS) nosetests -s

index:
	docker build -t agr_loader .
	docker run --publish=7474:7474 --publish=7687:7687 \
    --volume=./data:/data \
    --volume=./logs:/logs \
    neo4j:3.0