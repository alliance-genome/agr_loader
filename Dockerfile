ARG DOCKER_PULL_TAG=latest
ARG REG=agrdocker
FROM ${REG}/agr_base_linux_env:${DOCKER_PULL_TAG}

WORKDIR /usr/src/app

ADD . .

#RUN wget -q https://s3.amazonaws.com/agr-build-files/Anaconda3-2022.10-Linux-x86_64.sh && bash -b -p /usr/src/app/anaconda Anaconda3-2022.10-Linux-x86_64.sh
RUN wget -q https://repo.anaconda.com/miniconda/Miniconda3-py310_22.11.1-1-Linux-x86_64.sh && bash Miniconda3-py310_22.11.1-1-Linux-x86_64.sh -b

ENV PATH="${PATH}:/root/miniconda3/bin"

RUN conda env create -f conda_env.yml
SHELL ["conda", "run", "-n", "agr_loader", "/bin/bash", "-c"]

RUN conda run -n agr_loader pip install -r requirements.txt

RUN mkdir -p /var/lib/neo4j/import

CMD ["conda", "run", "-n", "agr_loader", "--no-capture-output", "python3", "src/aggregate_loader.py"]
