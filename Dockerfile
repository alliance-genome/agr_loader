FROM python:3.6.1-alpine

WORKDIR /usr/src/app

ADD . .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "src/prototype_index.py"]