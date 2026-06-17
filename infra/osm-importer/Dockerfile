FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    sqlalchemy==2.0.0 \
    psycopg2-binary==2.9.9 \
    requests==2.31.0

COPY osm-importer.py /app/osm-importer.py

RUN chmod +x /app/osm-importer.py

CMD ["python", "/app/osm-importer.py"]
