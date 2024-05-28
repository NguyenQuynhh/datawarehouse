FROM apache/airflow:latest 

USER root
RUN apt-get update && \
    apt-get -y install git && \
    apt-get clean

USER airflow
RUN pip install --upgrade pip

# Switch to the airflow user to install the necessary dependencies
USER airflow
RUN pip install apache-airflow-providers-google && \
    pip install pandas && \
    pip install numpy && \
    pip install scikit-learn && \
    pip install matplotlib && \
    pip install seaborn && \
    pip install scikit-learn.cluster

# Finally, switch back to the airflow user
USER airflow
