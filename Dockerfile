FROM puckel/docker-airflow:1.10.9

COPY requirements.txt requirements.txt
USER root
RUN apt update && apt install -y \
    python3-dev \
    libpq-dev
RUN pip install -r requirements.txt