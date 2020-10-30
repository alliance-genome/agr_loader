ARG ALLIANCE_RELEASE=latest
ARG REG=agrdocker
FROM ${REG}/agr_base_linux_env:${ALLIANCE_RELEASE}

WORKDIR /usr/src/app

ADD requirements.txt .

RUN pip3 install -r requirements.txt

ADD . .

RUN mkdir -p /var/lib/neo4j/import

CMD ["python3", "-u", "src/aggregate_loader.py"]
