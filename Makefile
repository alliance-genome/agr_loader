# get the Elasticsearch URI from an environment variable, if one is set
ES_HOST := $(or $(ES_HOST),$(ES_HOST),'127.0.0.1:9200')
ES_INDEX := $(or $(ES_INDEX),$(ES_INDEX),'searchable_items_blue')

OPTIONS = ES_HOST=$(ES_HOST) ES_AWS=$(ES_AWS) ES_INDEX=$(ES_INDEX)

install:
	pip install -r requirements.txt

index:
	cd src && $(OPTIONS) python fetch_index.py

test_index:
	cd src && $(OPTIONS) python fetch_test_index.py

test:
	$(OPTIONS) nosetests -s