REG=100225593120.dkr.ecr.us-east-1.amazonaws.com
ALLIANCE_RELEASE=0.0.0
DOCKER_PULL_TAG=build
DOCKER_BUILD_TAG=latest

registry-docker-login:
ifneq ($(shell echo ${REG} | egrep "ecr\..+\.amazonaws\.com"),)
	@$(eval DOCKER_LOGIN_CMD=docker run --rm -it -v ~/.aws:/root/.aws amazon/aws-cli)
ifneq (${AWS_PROFILE},)
	@$(eval DOCKER_LOGIN_CMD=${DOCKER_LOGIN_CMD} --profile ${AWS_PROFILE})
endif
	@$(eval DOCKER_LOGIN_CMD=${DOCKER_LOGIN_CMD} ecr get-login-password | docker login -u AWS --password-stdin https://${REG})
	${DOCKER_LOGIN_CMD}
endif

build: pull
	docker build --build-arg REG=${REG} --build-arg DOCKER_PULL_TAG=${DOCKER_PULL_TAG} -t ${REG}/agr_loader_run:${DOCKER_BUILD_TAG} .

buildenv: build

startdb:
	REG=${REG} DOCKER_PULL_TAG=${DOCKER_PULL_TAG} docker-compose up -d neo4j

stopdb:
	docker-compose stop neo4j

pull: registry-docker-login
	docker pull ${REG}/agr_neo4j_env:${DOCKER_PULL_TAG}
	docker pull ${REG}/agr_base_linux_env:${DOCKER_PULL_TAG}

removedb:
	docker-compose down -v

run: build
	REG=${REG} DOCKER_BUILD_TAG=${DOCKER_BUILD_TAG} ALLIANCE_RELEASE=${ALLIANCE_RELEASE} docker-compose up agr_loader

run_test: build
	REG=${REG} DOCKER_BUILD_TAG=${DOCKER_BUILD_TAG} ALLIANCE_RELEASE=${ALLIANCE_RELEASE} docker-compose run agr_loader_test
	REG=${REG} DOCKER_BUILD_TAG=${DOCKER_BUILD_TAG} ALLIANCE_RELEASE=${ALLIANCE_RELEASE} docker-compose run agr_loader_test_unit_tests

run_test_sub_load: build
	REG=${REG} TEST_CONFIG_OVERIDE=${TEST_CONFIG_OVERIDE} DOCKER_BUILD_TAG=${DOCKER_BUILD_TAG} ALLIANCE_RELEASE=${ALLIANCE_RELEASE} docker-compose run agr_loader_test

quick_unit_test: build
	docker run --rm ${REG}/agr_loader_run:${DOCKER_BUILD_TAG} pytest src/test/unit_tests.py

unit_tests:
	REG=${REG} DOCKER_BUILD_TAG=${DOCKER_BUILD_TAG} ALLIANCE_RELEASE=${ALLIANCE_RELEASE} docker-compose run agr_loader_test_unit_tests

run_loader_bash:
	docker run --rm -it --volume agr_loader_agr_data_share:/usr/src/app/tmp -e TEST_SET=True ${REG}/agr_loader_run:${DOCKER_BUILD_TAG} bash

# reload targets do remove and re-download files to the local docker volume.
reloaddb: 
	@${MAKE} --no-print-directory stopdb
	@${MAKE} --no-print-directory removedb
	@${MAKE} --no-print-directory startdb

reload: 
	@${MAKE} --no-print-directory startdb
	@${MAKE} --no-print-directory removedb
	@${MAKE} --no-print-directory startdb
	sleep 10
	@${MAKE} --no-print-directory run

reload_test: 
	@${MAKE} --no-print-directory startdb
	@${MAKE} --no-print-directory removedb
	@${MAKE} --no-print-directory startdb
	sleep 10
	@${MAKE} --no-print-directory build
	REG=${REG} docker-compose up agr_loader_test

# rebuild targets do not remove and re-download files to the local docker volume.
rebuild:
	@${MAKE} --no-print-directory startdb
	docker-compose down
	@${MAKE} --no-print-directory startdb
	sleep 10
	@${MAKE} --no-print-directory build
	REG=${REG} docker-compose up agr_loader

rebuild_test:
	@${MAKE} --no-print-directory startdb
	docker-compose down
	@${MAKE} --no-print-directory startdb
	sleep 10
	@${MAKE} --no-print-directory build
	REG=${REG} docker-compose up agr_loader_test
