FROM agrdocker/agr_loader_env

WORKDIR /usr/src/app

ADD . .

ARG test_set=False

CMD ["python", "-u", "src/fetch_index.py"]
