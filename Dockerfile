ARG DOCKER_PULL_TAG=latest
ARG REG=agrdocker
FROM ${REG}/agr_base_linux_env:${DOCKER_PULL_TAG}

WORKDIR /usr/src/app

ADD conda_env.yml conda_env.yml

RUN conda env create -f conda_env.yml
SHELL ["conda", "run", "-n", "agr_loader", "/bin/bash", "-c"]

ADD requirements.txt requirements.txt
RUN conda run -n agr_loader pip install -r requirements.txt

ADD . .

RUN mkdir -p /var/lib/neo4j/import

CMD ["conda", "run", "-n", "agr_loader", "--no-capture-output", "python3", "src/aggregate_loader.py"]
