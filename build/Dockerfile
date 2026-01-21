FROM apache/airflow:3.1.3

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
RUN pip install apache-airflow-providers-amazon