FROM apache/airflow:latest 

USER root
RUN apt-get update && \
    apt-get -y install git && \
    apt-get clean

USER airflow
RUN pip install --upgrade pip

# Switch to the airflow user to install the necessary dependencies
USER airflow
RUN pip install apache-airflow-providers-google

# Finally, switch back to the airflow user
USER airflow
