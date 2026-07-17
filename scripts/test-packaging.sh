#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHART_DIR="$ROOT_DIR/deploy/helm/rustium"
IMAGE="${RUSTIUM_PACKAGING_TEST_IMAGE:-rustium:packaging-test}"
VERSION="$(awk -F' = ' '/^version = / { gsub(/"/, "", $2); print $2; exit }' "$ROOT_DIR/Cargo.toml")"
REVISION="$(git -C "$ROOT_DIR" rev-parse HEAD)"
CREATED="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
RENDERED="$(mktemp)"
EXTERNAL_RENDERED="$(mktemp)"

cleanup() {
  rm -f "$RENDERED" "$EXTERNAL_RENDERED"
}
trap cleanup EXIT

command -v docker >/dev/null
command -v helm >/dev/null

# Validate every package manifest, included README, and format contract fixture
# without claiming unpublished internal crates already exist on crates.io.
for package in \
  rustium \
  rustium-column-transform \
  rustium-config \
  rustium-core \
  rustium-format-avro \
  rustium-format-json \
  rustium-format-protobuf \
  rustium-mysql \
  rustium-postgresql \
  rustium-server \
  rustium-signal-kafka \
  rustium-sink-kafka \
  rustium-sink-stdout \
  rustium-sqlserver \
  rustium-state; do
  package_files="$(cargo package -p "$package" --locked --allow-dirty --list)"
  grep -Fxq "README.md" <<<"$package_files"
  case "$package" in
    rustium-format-avro|rustium-format-json|rustium-format-protobuf)
      for connector in postgresql mysql sqlserver; do
        grep -Fxq "tests/fixtures/schema-${connector}-create.json" <<<"$package_files"
      done
      ;;
  esac
done

docker build \
  --pull \
  --build-arg "VERSION=$VERSION" \
  --build-arg "REVISION=$REVISION" \
  --build-arg "CREATED=$CREATED" \
  --tag "$IMAGE" \
  "$ROOT_DIR"

docker run --rm "$IMAGE" --version | grep -Fx "rustium $VERSION"

test "$(docker image inspect --format '{{.Config.User}}' "$IMAGE")" = "65532:65532"
test "$(docker image inspect --format '{{json .Config.Entrypoint}}' "$IMAGE")" = '["rustium"]'
test "$(docker image inspect --format '{{.Config.WorkingDir}}' "$IMAGE")" = "/var/lib/rustium"
test "$(docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' "$IMAGE")" = "$REVISION"

helm lint --strict "$CHART_DIR"
helm template rustium "$CHART_DIR" --namespace rustium >"$RENDERED"

grep -Fq "replicas: 1" "$RENDERED"
grep -Fq "type: Recreate" "$RENDERED"
grep -Fq "runAsNonRoot: true" "$RENDERED"
grep -Fq "readOnlyRootFilesystem: true" "$RENDERED"
grep -Fq "helm.sh/resource-policy: keep" "$RENDERED"
grep -Fq "path: /health/live" "$RENDERED"
grep -Fq "path: /health/ready" "$RENDERED"
grep -Fq "mountPath: /var/lib/rustium" "$RENDERED"

helm template rustium "$CHART_DIR" \
  --namespace rustium \
  --set config.existingSecret=rustium-external-config \
  --set config.content= >"$EXTERNAL_RENDERED"
grep -Fq "secretName: rustium-external-config" "$EXTERNAL_RENDERED"
if grep -Fq "kind: Secret" "$EXTERNAL_RENDERED"; then
  echo "chart rendered a managed configuration Secret while config.existingSecret is set" >&2
  exit 1
fi

if helm template rustium "$CHART_DIR" --set replicaCount=2 >/dev/null 2>&1; then
  echo "chart accepted more than one source-position owner" >&2
  exit 1
fi

echo "Rustium container and Helm packaging gate passed"
