#!/bin/bash
# Start Script für Railway

# Virtual Environment aktivieren und Gunicorn starten
/opt/venv/bin/gunicorn --bind 0.0.0.0:$PORT --timeout 300 --workers 2 --worker-class sync backend_server:app
