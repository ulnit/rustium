ARG POSTGRES_VERSION=17
FROM postgres:${POSTGRES_VERSION}

ARG POSTGRES_VERSION
RUN apt-get -o Acquire::ForceIPv4=true update \
    && DEBIAN_FRONTEND=noninteractive apt-get -o Acquire::ForceIPv4=true install -y --no-install-recommends \
        "postgresql-${POSTGRES_VERSION}-pgvector" \
        "postgresql-${POSTGRES_VERSION}-postgis-3" \
        "postgresql-${POSTGRES_VERSION}-postgis-3-scripts" \
    && rm -rf /var/lib/apt/lists/* \
    && install -d -o postgres -g postgres -m 0755 /etc/postgresql/rustium-tls \
    && openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
        -subj /CN=rustium-test-ca \
        -keyout /etc/postgresql/rustium-tls/ca.key \
        -out /etc/postgresql/rustium-tls/ca.crt \
    && openssl req -newkey rsa:2048 -nodes \
        -subj /CN=localhost \
        -addext subjectAltName=DNS:localhost \
        -keyout /etc/postgresql/rustium-tls/server.key \
        -out /etc/postgresql/rustium-tls/server.csr \
    && printf 'subjectAltName=DNS:localhost\n' > /etc/postgresql/rustium-tls/server.ext \
    && openssl x509 -req -days 2 \
        -in /etc/postgresql/rustium-tls/server.csr \
        -CA /etc/postgresql/rustium-tls/ca.crt \
        -CAkey /etc/postgresql/rustium-tls/ca.key \
        -CAcreateserial \
        -extfile /etc/postgresql/rustium-tls/server.ext \
        -out /etc/postgresql/rustium-tls/server.crt \
    && openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
        -subj /CN=rustium-wrong-ca \
        -keyout /etc/postgresql/rustium-tls/wrong-ca.key \
        -out /etc/postgresql/rustium-tls/wrong-ca.crt \
    && chown -R postgres:postgres /etc/postgresql/rustium-tls \
    && chmod 0600 /etc/postgresql/rustium-tls/server.key /etc/postgresql/rustium-tls/ca.key /etc/postgresql/rustium-tls/wrong-ca.key
