FROM debian:bookworm-slim

# osm2pgsql 1.x from bookworm doesn't support --schema flag
# Install 2.x from bookworm-backports
RUN echo "deb http://deb.debian.org/debian bookworm-backports main" >> /etc/apt/sources.list \
    && apt-get update && apt-get install -y --no-install-recommends \
        osm2pgsql/bookworm-backports \
        osmctools \
        ca-certificates \
        wget \
        postgresql-client \
    && rm -rf /var/lib/apt/lists/*

COPY import.sh /import.sh
COPY osm2pgsql.style /osm2pgsql.style

RUN chmod +x /import.sh

ENTRYPOINT ["/import.sh"]
