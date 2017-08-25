FROM agrdocker/agr_loader_env:develop

WORKDIR /usr/src/app

ADD . .

CMD ["python3", "-u", "src/fetch_index.py"]
