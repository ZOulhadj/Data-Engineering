FROM apache/airflow:2.3.0
COPY kaggle.json /var
RUN pip3 install opendatasets
RUN pip3 install pyspark
COPY requirements.txt .
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential libopenmpi-dev \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* \
RUN pip3 install -r requirements.txt
USER airflow
