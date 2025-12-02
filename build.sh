#!/bin/bash
# Railway Build Script für Backend

echo "================================================"
echo "   BACKEND BUILD - PySpark Dashboard"
echo "================================================"

# Python Dependencies installieren
echo "📦 Installiere Python Dependencies..."
pip install --no-cache-dir -r backend_requirements.txt

# Verzeichnisse erstellen
echo "📁 Erstelle Upload-Verzeichnis..."
mkdir -p uploads

# Build abgeschlossen
echo "✅ Build erfolgreich abgeschlossen!"
echo "================================================"

