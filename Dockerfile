FROM agrdocker/agr_loader_env

WORKDIR /usr/src/app

ADD . .

CMD ["python", "src/prototype_index.py"]
