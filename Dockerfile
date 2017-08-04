FROM agrdocker/agr_loader_env:develop

WORKDIR /usr/src/app

ADD . .

CMD ["python", "-u", "src/fetch_index.py"]
