ARG POSTGRES_VERSION=17
FROM postgres:${POSTGRES_VERSION}

ARG POSTGRES_VERSION
RUN apt-get -o Acquire::ForceIPv4=true update \
    && DEBIAN_FRONTEND=noninteractive apt-get -o Acquire::ForceIPv4=true install -y --no-install-recommends \
        "postgresql-${POSTGRES_VERSION}-pgvector" \
        "postgresql-${POSTGRES_VERSION}-postgis-3" \
        "postgresql-${POSTGRES_VERSION}-postgis-3-scripts" \
    && rm -rf /var/lib/apt/lists/*
