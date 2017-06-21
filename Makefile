# get the Elasticsearch URI from an environment variable, if one is set
CURRENT_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

index:
	docker build -t agr_loader .
	docker run --publish=7474:7474 --publish=7687:7687 \
	--volume=$(CURRENT_DIR)/data:/data \
	--volume=$(CURRENT_DIR)/logs:/logs \
	neo4j:3.0