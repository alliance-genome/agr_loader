ARG DOCKER_PULL_TAG=latest
ARG REG=agrdocker
FROM ${REG}/agr_base_linux_env:${DOCKER_PULL_TAG}

WORKDIR /usr/src/app

ADD . .

RUN conda env create -f conda_env.yml
SHELL ["conda", "run", "-n", "agr_loader", "/bin/bash", "-c"]

RUN conda run -n agr_loader pip install -r requirements.txt

RUN mkdir -p /var/lib/neo4j/import

CMD ["conda", "run", "-n", "agr_loader", "--no-capture-output", "python3", "src/aggregate_loader.py"]
