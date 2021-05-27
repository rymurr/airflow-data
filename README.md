## Steps

1. build docker image `docker build .`
2. start docker compose `AIRFLOW_IMAGE_NAME={hash from above} docker-compose up`
3. navigate to http://localhost:8081 log in with airflow:airflow
4. go to Admin -> Connections and add `nessie-default` as a `Nessie` connection type (host = http://nessie:19120api/v1)
5. go to Admin -> Connections and add `spark-cluster` and `spark-cluster-sql` (host = spark://spark and port= 7077). types are spark and spark_sql respectively
6. go to Admin -> Connections and add `aws-nessie` as an `aws` type with user=access_key and pass=secret_key
7. run the example_spark_operator dag


## Airflow provider

* nessie_provider.hooks.nessie_hook - Defines a Hook in Airflow and exposes a connection in the UI
* nessie_provider.operators.create - runs pynessie to create a ref
* nessie_provider.operatprs.merge - runs pynessie to execute a merge

## Example job

* Create branch
* run spark jobs to add two tables to branch
* merge branch
* delete branch

## Still to do

1. figure out how to realistically handle the `NessieSparkSql` job
2. possibly create a sensor and create a job that uses it. Sensor could be a) wait for table to change or b) wait for commit on branch for example
3. possibly add operators to expose more Nessie functionality
4. correctly package and push to PyPI - correct names, correct docs, typing, black, testing etc etc
5. add `packages` and `env` to spark sql operator on airflow github
6. SparkSql operator is annoying, it appears you can only run one at a time as it does somehitng funky w/ a derby db. May just want to use spark-submit?
7. is there a way to submit sql via a rest api or something? or via a pyspark job? Probably w databricks
