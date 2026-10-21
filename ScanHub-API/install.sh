#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BACSHOME="$(dirname "${SCRIPT_DIR}")"
VENV="${BACSHOME}/ScanHub-API-venv"

echo "Installing Python 3.11..."
dnf install -y python3.11 python3.11-pip

echo "Creating ScanHub virtual environment..."
python3.11 -m venv "${VENV}"

echo "Upgrading pip..."
"${VENV}/bin/python" -m pip install --upgrade pip

echo "Installing ScanHub requirements..."
"${VENV}/bin/python" -m pip install -r "${SCRIPT_DIR}/requirements.txt"

echo "Checking dependencies..."
"${VENV}/bin/python" -m pip check

echo
echo "ScanHub environment installed successfully."
echo

"${VENV}/bin/python" --version

"${VENV}/bin/python" -c \
"import fastapi, starlette, hypercorn, uvloop, pydantic, pyclamd, multipart, orjson; \
print('FastAPI:', fastapi.__version__); \
print('Starlette:', starlette.__version__); \
print('Pydantic:', pydantic.__version__); \
print('orjson:', orjson.__version__)"
