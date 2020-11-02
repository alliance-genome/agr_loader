ARG ALLIANCE_RELEASE=latest
ARG REG=agrdocker
FROM 100225593120.dkr.ecr.us-east-1.amazonaws.com/agr_base_linux_env:4.0.0

WORKDIR /usr/src/app

ADD requirements.txt .

RUN pip3 install -r requirements.txt

ADD . .

RUN mkdir -p /var/lib/neo4j/import

CMD ["python3", "-u", "src/aggregate_loader.py"]
