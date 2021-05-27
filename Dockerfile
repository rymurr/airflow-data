FROM apache/airflow

USER 0
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-11-jre
RUN java --version
RUN mkdir -p /opt/spark
RUN chown 50000:50000 /opt/spark
USER 50000

RUN pip install apache-airflow-providers-apache-spark
RUN curl -O https://downloads.apache.org/spark/spark-3.0.2/spark-3.0.2-bin-hadoop2.7.tgz
RUN tar xf spark-3.0.2-bin-hadoop2.7.tgz -C /opt/spark

ENV SPARK_HOME="/opt/spark/spark-3.0.2-bin-hadoop2.7"
ENV PATH="/opt/spark/spark-3.0.2-bin-hadoop2.7/bin:${PATH}"

USER 0
RUN mkdir -p /home/airflow/.ivy2
RUN chown 50000:50000 /home/airflow/.ivy2
RUN chmod 777 /home/airflow/.ivy2
RUN mkdir -p /home/airflow/.config
RUN chown 50000:50000 /home/airflow/.config
RUN chmod 777 /home/airflow/.config
USER 50000

ADD nessie_provider/ nessie_provider/
ADD setup.py .
RUN pip install . 

ENTRYPOINT ["/usr/bin/dumb-init", "--", "/entrypoint"]
CMD ["--help"]
