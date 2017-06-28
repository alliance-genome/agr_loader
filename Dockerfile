FROM agrdocker/agr_loader_env

WORKDIR /usr/src/app

ADD . .

CMD ["python", "src/fetch_index.py"]
