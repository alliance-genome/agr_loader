FROM christabone/homeone:agr_loader_env_0.3
FROM agrdocker/neo4j

WORKDIR /src
ADD src /src

CMD ["python", "prototype_index.py"]