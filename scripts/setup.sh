#!/usr/bin/env bash
# Install the project environment on an Apple Silicon Mac.
set -euo pipefail
cd "$(dirname "$0")/.."
if [ "$(uname -s)-$(uname -m)" != "Darwin-arm64" ]; then
  echo "This setup script targets macOS Apple Silicon. See README for portability notes." >&2
  exit 1
fi
command -v uv >/dev/null || { echo "Install uv first: brew install uv" >&2; exit 1; }
mkdir -p .runtime/downloads
uv python install 3.11.17
if [ ! -x .venv/bin/python ]; then
  uv venv --python 3.11.17 --managed-python .venv
fi
uv pip sync --python .venv/bin/python requirements-lock.txt
if [ ! -x .runtime/jdk-17.0.20.1+1/Contents/Home/bin/java ]; then
  curl -fL --retry 2 -o .runtime/downloads/java.tar.gz \
    'https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.20.1%2B1/OpenJDK17U-jdk_aarch64_mac_hotspot_17.0.20.1_1.tar.gz'
  echo '196d13ba5f10414bef7f6a05a9b3f00edacb18ebacef2b99485db9e2ee18f0e8  .runtime/downloads/java.tar.gz' | shasum -a 256 -c -
  tar -xzf .runtime/downloads/java.tar.gz -C .runtime
fi
if [ ! -x .runtime/kafka_2.13-3.9.1/bin/kafka-server-start.sh ]; then
  curl -fL --retry 2 -o .runtime/downloads/kafka.tgz \
    https://archive.apache.org/dist/kafka/3.9.1/kafka_2.13-3.9.1.tgz
  echo '1ea204ba73411737a275429ca976d440f007ff0957b90b19be41dc5a4bae52617267769be9f0b5791714d0b3c4c760605bd426faea39edd90763585523fa2cfe  .runtime/downloads/kafka.tgz' | shasum -a 512 -c -
  tar -xzf .runtime/downloads/kafka.tgz -C .runtime
fi
source scripts/env.sh
.venv/bin/python -m ipykernel install --user \
  --name assessment2-aml --display-name 'Python (assessment2-aml)' \
  --env JAVA_HOME "$JAVA_HOME" --env PYSPARK_PYTHON "$PYSPARK_PYTHON" \
  --env SPARK_LOCAL_IP "$SPARK_LOCAL_IP" --env PYSPARK_SUBMIT_ARGS "$PYSPARK_SUBMIT_ARGS"
echo 'Ready. Start Kafka and Jupyter in separate terminals using the README commands.'
