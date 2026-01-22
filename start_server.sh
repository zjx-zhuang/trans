#!/bin/bash
# Start the FastAPI server with HTTPS and external access

# Generate certs if they don't exist
if [ ! -f certs/key.pem ] || [ ! -f certs/cert.pem ]; then
    echo "Generating SSL certificates..."
    mkdir -p certs
    /usr/bin/openssl req -x509 -newkey rsa:4096 -keyout certs/key.pem -out certs/cert.pem -days 365 -nodes -subj '/CN=localhost'
fi

echo "Starting server on https://0.0.0.0:8000"
# Use the python executable from the environment (miniconda3)
/Users/apple/miniconda3/bin/python3 -m uvicorn src.main:app --host 0.0.0.0 --port 8000 --ssl-keyfile certs/key.pem --ssl-certfile certs/cert.pem --reload
