#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
postgres_version=${RUSTIUM_POSTGRES_EXTENSION_VERSION:-17}
image=${RUSTIUM_POSTGRES_EXTENSION_IMAGE:-rustium/postgres-extensions:${postgres_version}}
container="rustium-pg-extensions-${postgres_version}-$$"
database=cdc_demo
password=rustium-extension-test
dockerfile="$root/crates/rustium-postgresql/tests/postgresql-extensions.Dockerfile"

cleanup() {
    docker rm -f "$container" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

docker build \
    --build-arg "POSTGRES_VERSION=$postgres_version" \
    --tag "$image" \
    --file "$dockerfile" \
    "$root/crates/rustium-postgresql/tests"

docker run --detach \
    --name "$container" \
    --env "POSTGRES_PASSWORD=$password" \
    --env "POSTGRES_DB=$database" \
    --publish 127.0.0.1::5432 \
    "$image" \
    postgres \
        -c wal_level=logical \
        -c max_replication_slots=20 \
        -c max_wal_senders=20 \
    >/dev/null

ready=false
for _ in $(seq 1 60); do
    if docker exec "$container" sh -c \
        'test "$(head -n 1 "$PGDATA/postmaster.pid")" = 1' >/dev/null 2>&1 \
        && docker exec "$container" psql -v ON_ERROR_STOP=1 -Atq \
            -U postgres -d "$database" -c 'SELECT 1' >/dev/null 2>&1; then
        ready=true
        break
    fi
    sleep 1
done
if [[ $ready != true ]]; then
    docker logs "$container"
    exit 1
fi

docker exec "$container" psql -v ON_ERROR_STOP=1 -U postgres -d "$database" \
    -c 'CREATE EXTENSION vector; CREATE EXTENSION postgis;'

mapping=$(docker port "$container" 5432/tcp)
port=${mapping##*:}

cd "$root"
RUSTIUM_POSTGRES_TEST_HOST=127.0.0.1 \
RUSTIUM_POSTGRES_TEST_PORT="$port" \
RUSTIUM_POSTGRES_TEST_USER=postgres \
RUSTIUM_POSTGRES_TEST_PASSWORD="$password" \
RUSTIUM_POSTGRES_TEST_DATABASE="$database" \
RUSTIUM_POSTGRES_REQUIRE_EXTENSION_TYPES=true \
cargo test -p rustium-postgresql --test postgresql_external --locked -- \
    keeps_installed_extension_types_identical_across_snapshot_and_wal \
    --ignored --exact --nocapture

RUSTIUM_POSTGRES_TEST_HOST=127.0.0.1 \
RUSTIUM_POSTGRES_TEST_PORT="$port" \
RUSTIUM_POSTGRES_TEST_USER=postgres \
RUSTIUM_POSTGRES_TEST_PASSWORD="$password" \
RUSTIUM_POSTGRES_TEST_DATABASE="$database" \
RUSTIUM_POSTGRES_RECONNECT_SOAK_CYCLES="${RUSTIUM_POSTGRES_RECONNECT_SOAK_CYCLES:-3}" \
cargo test -p rustium-postgresql --test postgresql_external --locked -- \
    reconnects_after_replication_backend_termination \
    --ignored --exact --nocapture

RUSTIUM_POSTGRES_TEST_HOST=127.0.0.1 \
RUSTIUM_POSTGRES_TEST_PORT="$port" \
RUSTIUM_POSTGRES_TEST_USER=postgres \
RUSTIUM_POSTGRES_TEST_PASSWORD="$password" \
RUSTIUM_POSTGRES_TEST_DATABASE="$database" \
cargo test -p rustium-postgresql --test postgresql_external --locked -- \
    filters_initial_snapshot_without_narrowing_streaming \
    --ignored --exact --nocapture

RUSTIUM_POSTGRES_TEST_HOST=127.0.0.1 \
RUSTIUM_POSTGRES_TEST_PORT="$port" \
RUSTIUM_POSTGRES_TEST_USER=postgres \
RUSTIUM_POSTGRES_TEST_PASSWORD="$password" \
RUSTIUM_POSTGRES_TEST_DATABASE="$database" \
cargo test -p rustium-postgresql --test postgresql_external --locked -- \
    manages_debezium_publication_autocreate_modes \
    --ignored --exact --nocapture

RUSTIUM_POSTGRES_TEST_HOST=127.0.0.1 \
RUSTIUM_POSTGRES_TEST_PORT="$port" \
RUSTIUM_POSTGRES_TEST_USER=postgres \
RUSTIUM_POSTGRES_TEST_PASSWORD="$password" \
RUSTIUM_POSTGRES_TEST_DATABASE="$database" \
cargo test -p rustium-postgresql --test postgresql_external --locked -- \
    applies_debezium_replica_identity_autoset_values_atomically \
    --ignored --exact --nocapture

RUSTIUM_POSTGRES_TEST_HOST=127.0.0.1 \
RUSTIUM_POSTGRES_TEST_PORT="$port" \
RUSTIUM_POSTGRES_TEST_USER=postgres \
RUSTIUM_POSTGRES_TEST_PASSWORD="$password" \
RUSTIUM_POSTGRES_TEST_DATABASE="$database" \
cargo test -p rustium-postgresql --test postgresql_external --locked -- \
    publishes_partition_changes_via_the_partition_root \
    --ignored --exact --nocapture
