FROM agrdocker/agr_loader_env

WORKDIR /usr/src/app

ADD . .

CMD ["python", "-u", "src/fetch_index.py"]
