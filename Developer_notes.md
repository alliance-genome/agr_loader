# Unit Tests
This tests the methods and no neo4j database is needed. Can be used for quickly
testing methods.

```bash
make quick_unit_test
```

# Alt config files
We can now add specific config files to just load specific data, better to have a list of 
these as these give an idea of what is needed for each and saves time later as we do not 
have to alter the same file test.yml each time. test.yml is used by default if 
TEST_CONFIG_OVERIDE is not set so should not be a problem for anyone else.

i.e.

export TEST_CONFIG_OVERIDE=sub_loads/interactions_only.yml

# Interactive debugging
To start an interactive container to look at data etc...
```bash
make run_loader_bash
```

