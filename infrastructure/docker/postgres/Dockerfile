FROM postgres:16-alpine

# Set environment variables
ENV POSTGRES_USER=bornemap
ENV POSTGRES_PASSWORD=bomeapassword
ENV POSTGRES_DB=bornemap

# Copy custom init scripts
COPY infrastructure/postgres/init/ /docker-entrypoint-initdb.d/

# Expose PostgreSQL port
EXPOSE 5432

# Health check
HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=3 \
    CMD pg_isready -U $POSTGRES_USER || exit 1
