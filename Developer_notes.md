#Unit Tests
This tests the methods and no neo4j database is needed. Can be used for quickly
testing methods.

docker build -t agrdocker/agr_loader_run:latest .
 docker run --rm agrdocker/agr_loader_run pytest src/test/unit_tests.py

#Alt config files
We can now add specific config files to just load specific data, better to have a list of 
these as these give an idea of what is needed for each and saves time later as we do not 
have to alter the same file test.yml each time. test.yml is used by default if 
TEST_CONFIG_OVERIDE is not set so should not be a problem for anyone else.

i.e.

export TEST_CONFIG_OVERIDE=sub_loads/interactions_only.yml


docker  run -it --volume agr_loader_agr_data_share:/usr/src/app/tmp -e TEST_SET=True agrdocker/agr_loader_run bash