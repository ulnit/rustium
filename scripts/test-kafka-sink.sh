#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
image=${RUSTIUM_KAFKA_TEST_IMAGE:-docker.redpanda.com/redpandadata/redpanda:v25.1.9}
port=${RUSTIUM_KAFKA_TEST_PORT:-19092}
container="rustium-kafka-sink-$$"

cleanup() {
    status=$?
    trap - EXIT INT TERM
    if [[ $status -ne 0 ]]; then
        docker logs "$container" 2>/dev/null || true
    fi
    docker rm -f "$container" >/dev/null 2>&1 || true
    exit "$status"
}
trap cleanup EXIT INT TERM

docker pull "$image"
docker run --detach \
    --name "$container" \
    --publish "127.0.0.1:${port}:${port}" \
    "$image" \
    redpanda start \
        --mode dev-container \
        --smp 1 \
        --memory 512M \
        --reserve-memory 0M \
        --node-id 0 \
        --check=false \
        --kafka-addr "PLAINTEXT://0.0.0.0:${port}" \
        --advertise-kafka-addr "PLAINTEXT://127.0.0.1:${port}" \
    >/dev/null

ready=false
for _ in $(seq 1 60); do
    if docker exec "$container" \
        rpk cluster info -X "brokers=127.0.0.1:${port}" >/dev/null 2>&1; then
        ready=true
        break
    fi
    sleep 1
done
if [[ $ready != true ]]; then
    echo "Kafka broker did not become ready" >&2
    exit 1
fi

cd "$root"
RUSTIUM_KAFKA_TEST_BOOTSTRAP_SERVERS="127.0.0.1:${port}" \
cargo test -p rustium-sink-kafka --test kafka_external --locked -- \
    delivers_records_tombstones_and_broker_failures \
    --ignored --exact --nocapture
