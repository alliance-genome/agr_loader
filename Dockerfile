FROM agrdocker/agr_python_env:latest

WORKDIR /usr/src/app

ADD requirements.txt .

RUN pip install -r requirements.txt

ADD . .

CMD ["python3", "-u", "src/fetch_index.py"]
