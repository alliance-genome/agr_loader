REG=100225593120.dkr.ecr.us-east-1.amazonaws.com
TAG=latest

registry-docker-login:
ifneq ($(shell echo ${REG} | egrep "ecr\..+\.amazonaws\.com"),)
	@$(eval DOCKER_LOGIN_CMD=aws)
ifneq (${AWS_PROFILE},)
	@$(eval DOCKER_LOGIN_CMD=${DOCKER_LOGIN_CMD} --profile ${AWS_PROFILE})
endif
	@$(eval DOCKER_LOGIN_CMD=${DOCKER_LOGIN_CMD} ecr get-login-password | docker login -u AWS --password-stdin https://${REG})
	${DOCKER_LOGIN_CMD}
endif

build: pull
	docker build --build-arg REG=${REG} -t ${REG}/agr_loader_run:${TAG} .

buildenv: build

startdb:
	REG=${REG} docker-compose up -d neo4j

stopdb:
	docker-compose stop neo4j

pull: registry-docker-login
	docker pull ${REG}/agr_neo4j_env:${TAG}
	docker pull ${REG}/agr_base_linux_env:${TAG}

removedb:
	docker-compose down -v

run: build
	REG=${REG} docker-compose up agr_loader

run_test_travis:  
	build
	REG=${REG} docker-compose run agr_loader_travis
	REG=${REG} docker-compose run agr_loader_test_unit_tests

run_test: build
	REG=${REG} docker-compose run agr_loader_test
	REG=${REG} docker-compose run agr_loader_test_unit_tests

quick_unit_test: build
	docker run --rm ${REG}/agr_loader_run pytest src/test/unit_tests.py

unit_tests:
	REG=${REG} docker-compose run agr_loader_test_unit_tests

bash:
	REG=${REG} docker-compose up agr_loader bash

# reload targets do remove and re-download files to the local docker volume.
reload: 
	@${MAKE} --no-print-directory startdb
	@${MAKE} --no-print-directory removedb
	@${MAKE} --no-print-directory startdb
	sleep 10
	@${MAKE} --no-print-directory run

reload_test: 
	@${MAKE} --no-print-directory startdb
	docker-compose down
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

run_loader_bash:
	docker run --rm -it --volume agr_loader_agr_data_share:/usr/src/app/tmp -e TEST_SET=True ${REG}/agr_loader_run bash
