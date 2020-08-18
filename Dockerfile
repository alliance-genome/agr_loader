FROM agrdocker/agr_base_linux_env:latest

WORKDIR /usr/src/app

ADD requirements.txt .

RUN apt-get update

RUN apt-get -yq install gcc python3-dev

RUN pip3 install -r requirements.txt

ADD . .

RUN mkdir -p /var/lib/neo4j/import

CMD ["python3", "-u", "src/aggregate_loader.py"]
