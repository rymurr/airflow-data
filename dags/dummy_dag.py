import os
from typing import Dict, Any

from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.utils.dates import days_ago
from nessie_provider.hooks.nessie_hook import NessieHook
from nessie_provider.operators.create_branch_operator import CreateBranchOperator
from nessie_provider.operators.merge_branch_operator import MergeOperator


def monkey_patch(env_vars):
    def execute(self, context: Dict[str, Any]) -> None:
        """Call the SparkSqlHook to run the provided sql query"""
        env = os.environ.copy()
        env.update(env_vars)
        kwargs = {}
        kwargs["env"] = env
        if self._hook is None:
            self._hook = self._get_hook()
        self._hook.run_query("--packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7,org.apache.iceberg:iceberg-spark3-runtime:0.11.1,software.amazon.awssdk:s3:2.15.40,software.amazon.awssdk:url-connection-client:2.15.40,software.amazon.awssdk:kms:2.15.40,software.amazon.awssdk:glue:2.15.40,software.amazon.awssdk:dynamodb:2.15.40", **kwargs)
    SparkSqlOperator.execute = execute


# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
}
with DAG(
        dag_id='example_spark_operator',
        default_args=default_args,
        schedule_interval=None,
        start_date=days_ago(2),
        tags=['example', 'spark'],
) as dag:
    creds = AwsBaseHook('aws-nessie').get_credentials("eu-central-1")
    env_vars = {
        'AWS_ACCESS_KEY_ID': creds.access_key,
        'AWS_SECRET_ACCESS_KEY': creds.secret_key,
        'AWS_REGION': 'eu-central-1',
        'AWS_DEFAULT_REGION': 'eu-central-1',
        "ENABLE_S3_SIGV4_SYSTEM_PROPERTY": "true",
    }
    monkey_patch(env_vars)

    nessie_conn = NessieHook("nessie-default").get_connection("nessie-default")
    conf = {
        "spark.sql.execution.pyarrow.enabled":
        "true",
        "spark.sql.catalog.nessie.warehouse":
        's3://rymurr.test.bucket/',
        "spark.sql.catalog.nessie.io-impl":
        "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.extensions":
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.nessie":
        "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.nessie.catalog-impl":
            "org.apache.iceberg.nessie.NessieCatalog",
        "spark.sql.catalog.nessie.url":
            nessie_conn.host,
        "spark.sql.catalog.nessie.uri":
            nessie_conn.host,
        "spark.sql.catalog.nessie.ref":
            nessie_conn.schema,
        "spark.hadoop.fs.s3a.impl":
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.com.amazonaws.services.s3.enableV4":
            "true",
        "spark.hadoop.fs.s3a.endpoint":
            "s3.eu-central-1.amazonaws.com",
        "spark.hadoop.fs.s3a.access.key":
            creds.access_key,
        "spark.hadoop.fs.s3a.secret.key":
            creds.secret_key,
        "spark.driver.extraJavaOptions":
        '-Dcom.amazonaws.services.s3.enableV4=true -Daws.region=eu-central-1 -Daws.accessKeyId={} -Daws.secretAccessKey={}'.format(creds.access_key,creds.secret_key),
        "spark.executor.extraJavaOptions":
        '-Dcom.amazonaws.services.s3.enableV4=true -Daws.region=eu-central-1 -Daws.accessKeyId={} -Daws.secretAccessKey={}'.format(creds.access_key,creds.secret_key),
    }

    submit_job = SparkSqlOperator(
        conf=','.join("{}={}".format(k,v) for k,v in conf.items()),
        sql="""CREATE TEMPORARY VIEW parquetTable
USING org.apache.spark.sql.parquet
OPTIONS (
  path "s3a://rymurr.test.bucket/region.parquet"
);CREATE TABLE nessie.testing.region USING iceberg AS SELECT * FROM parquetTable""",
        conn_id='spark-cluster-sql',
        task_id="region",
        master="spark://spark:7077",
        name="test-job-region"
    )

    submit_job_2 = SparkSubmitOperator(
        conf=conf,
        application=os.environ["AIRFLOW_HOME"] + "/dags/test.py",
        conn_id='spark-cluster',
        task_id="nation",
        name="test-job-nation",
        env_vars=env_vars,
        packages="com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7,org.apache.iceberg:iceberg-spark3-runtime:0.11.1,software.amazon.awssdk:s3:2.15.40,software.amazon.awssdk:url-connection-client:2.15.40,software.amazon.awssdk:kms:2.15.40,software.amazon.awssdk:glue:2.15.40,software.amazon.awssdk:dynamodb:2.15.40",
    )

    create_branch = CreateBranchOperator("nessie-default", "test", task_id="create")
    merge_branch = MergeOperator("nessie-default", "test", task_id="merge")
    create_branch >> submit_job >> merge_branch
    create_branch >> submit_job_2 >> merge_branch

