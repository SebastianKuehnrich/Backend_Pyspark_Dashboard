# ============================================
# Flask Backend für Dashboard
# CSV Upload + PySpark Analyse
# ============================================

from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import os
import json
import subprocess
import tempfile
import shutil
from datetime import datetime

# Frontend-Pfad anpassen (liegt jetzt in ../frontend/dist)
FRONTEND_DIST = os.path.join(os.path.dirname(__file__), '..', 'frontend', 'dist')

app = Flask(__name__, static_folder=FRONTEND_DIST, static_url_path='')

# CORS konfigurieren - Frontend URLs erlauben
CORS(app, resources={
    r"/api/*": {
        "origins": [
            "https://pysparkkundenanalysedashboard-production.up.railway.app",
            "http://localhost:5173",
            "http://localhost:5000"
        ]
    }
})

# Konfiguration
UPLOAD_FOLDER = 'uploads'
DATA_FILE = 'data.json'
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE_PATH = os.path.join(SCRIPT_DIR, DATA_FILE)
PYSPARK_SCRIPT = os.path.join(SCRIPT_DIR, 'generate_dashboard_data.py')
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

@app.route('/')
def serve_dashboard():
    """Serve React Dashboard"""
    try:
        return send_from_directory(FRONTEND_DIST, 'index.html')
    except FileNotFoundError:
        return jsonify({
            'error': 'Frontend nicht gefunden',
            'message': 'Bitte führe zuerst "npm run build" im frontend Ordner aus'
        }), 404

@app.route('/api/upload-csv', methods=['POST'])
def upload_csv():
    """CSV hochladen und PySpark-Analyse starten"""
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'Keine Datei hochgeladen'}), 400

        file = request.files['file']

        if file.filename == '':
            return jsonify({'error': 'Keine Datei ausgewählt'}), 400

        if not file.filename.endswith('.csv'):
            return jsonify({'error': 'Nur CSV-Dateien erlaubt'}), 400

        # Speichere CSV temporär
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'ecommerce_{timestamp}.csv'
        filepath = os.path.join(UPLOAD_FOLDER, filename)
        file.save(filepath)

        # Erstelle temporäres PySpark-Script mit neuem Pfad
        temp_script = create_temp_pyspark_script(filepath)

        # Führe PySpark-Analyse aus
        result = subprocess.run(
            ['python', temp_script],
            capture_output=True,
            text=True,
            timeout=300  # 5 Minuten Timeout
        )

        # Lösche temporäres Script
        os.remove(temp_script)

        if result.returncode != 0:
            return jsonify({
                'error': 'Fehler bei PySpark-Analyse',
                'details': result.stderr
            }), 500

        # Lade generierte Daten
        with open(DATA_FILE_PATH, 'r', encoding='utf-8') as f:
            dashboard_data = json.load(f)

        return jsonify({
            'success': True,
            'message': 'Daten erfolgreich analysiert',
            'filename': filename,
            'data': dashboard_data
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/data', methods=['GET'])
def get_dashboard_data():
    """Aktuelle Dashboard-Daten abrufen"""
    try:
        with open(DATA_FILE_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return jsonify(data)
    except FileNotFoundError:
        return jsonify({
            'error': 'Keine Daten vorhanden',
            'message': 'Bitte laden Sie zuerst eine CSV-Datei hoch'
        }), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/status', methods=['GET'])
def get_status():
    """Server-Status prüfen"""
    return jsonify({
        'status': 'online',
        'version': '1.0.0',
        'timestamp': datetime.now().isoformat()
    })

def create_temp_pyspark_script(csv_path):
    """Erstelle temporäres PySpark-Script mit dynamischem CSV-Pfad"""

    # Lade Original-Script
    with open(PYSPARK_SCRIPT, 'r', encoding='utf-8') as f:
        original_script = f.read()

    # Ersetze DATA_PATH mit neuem Pfad
    modified_script = original_script.replace(
        'DATA_PATH = "C:/Users/sebas/PycharmProjects/BigData/daten/ecommerce_5m.csv"',
        f'DATA_PATH = "{csv_path}"'
    )

    # Ändere auch den Output-Pfad für data.json
    modified_script = modified_script.replace(
        './ergebnisse/',
        f'{SCRIPT_DIR}/'
    )

    # Speichere temporäres Script
    temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False, encoding='utf-8')
    temp_file.write(modified_script)
    temp_file.close()

    return temp_file.name

if __name__ == '__main__':
    print("=" * 60)
    print("   KUNDEN-ANALYSE DASHBOARD - SERVER")
    print("=" * 60)
    print()
    print("Backend läuft auf: http://localhost:5000")
    print("Dashboard: http://localhost:5000")
    print()
    print("Drücke STRG+C zum Beenden")
    print("=" * 60)
    print()

    app.run(host='0.0.0.0', port=5000, debug=True)
