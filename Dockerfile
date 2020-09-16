FROM agrdocker/agr_base_linux_env:latest

WORKDIR /usr/src/app

ADD requirements.txt .

RUN pip3 install -r requirements.txt

ADD . .

RUN mkdir -p /var/lib/neo4j/import

CMD ["python3", "-u", "src/aggregate_loader.py"]
