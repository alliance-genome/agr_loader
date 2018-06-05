FROM agrdocker/agr_python_env:latest

WORKDIR /usr/src/app

ADD requirements.txt .

RUN pip3 install -r requirements.txt

ADD . .

CMD ["python3", "-u", "src/fetch_index.py"]
