#!/bin/bash
# aws_logs コマンドのエントリポイント
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export PYTHONPATH="${SCRIPT_DIR}:${PYTHONPATH:-}"

# venv の python を探す
PYTHON=$(command -v python3 || command -v python)
for vdir in .venv venv; do
    if [[ -f "${SCRIPT_DIR}/${vdir}/bin/python" ]]; then
        PYTHON="${SCRIPT_DIR}/${vdir}/bin/python"
        break
    fi
done

exec "$PYTHON" -m aws_logs "$@"
