FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir \
    sqlalchemy==2.0.0 \
    psycopg2-binary==2.9.9 \
    requests==2.31.0 \
    python-dotenv==1.0.0 \
    overpass-api==0.3.1

# Copy import script
COPY osm-importer.py /app/osm-importer.py

# Copy config template
COPY config.example.py /app/config.py.example

# Make script executable
RUN chmod +x /app/osm-importer.py

# Expose port
EXPOSE 8000

# Run the import script
CMD ["python", "/app/osm-importer.py"]
