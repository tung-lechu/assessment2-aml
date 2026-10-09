#!/usr/bin/env bash
# Load the Java and Python paths for this project.
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export JAVA_HOME="$PROJECT_ROOT/.runtime/jdk-17.0.20.1+1/Contents/Home"
export PYSPARK_PYTHON="$PROJECT_ROOT/.venv/bin/python"
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_SUBMIT_ARGS="--driver-memory 4g pyspark-shell"
export PATH="$JAVA_HOME/bin:$PROJECT_ROOT/.venv/bin:$PATH"
