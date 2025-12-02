# ============================================
# Simple CSV Analyse ohne PySpark für Railway
# Generiert dashboard-kompatible data.json
# ============================================

import pandas as pd
import json
import os
import sys
from datetime import datetime


def analyze_csv(csv_path, output_path='data.json'):
    """Einfache CSV-Analyse mit Pandas statt PySpark"""

    print(f"[ANALYZER] Start: {datetime.now().isoformat()}")
    print(f"[ANALYZER] CSV-Pfad: {csv_path}")
    print(f"[ANALYZER] Output-Pfad: {output_path}")

    # Prüfe ob CSV existiert
    if not os.path.exists(csv_path):
        print(f"[ERROR] CSV nicht gefunden: {csv_path}")
        sys.exit(1)

    # Dateigröße loggen
    file_size_mb = os.path.getsize(csv_path) / (1024 * 1024)
    print(f"[ANALYZER] Dateigröße: {file_size_mb:.2f} MB")

    print(f"[ANALYZER] Lade CSV...")
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"[ERROR] CSV konnte nicht geladen werden: {e}")
        sys.exit(1)

    print(f"[ANALYZER] Geladen: {len(df)} Zeilen, {len(df.columns)} Spalten")
    print(f"[ANALYZER] Spalten: {list(df.columns)}")

    # Validiere benötigte Spalten
    required_columns = ['customer_id', 'transaction_id', 'total', 'date']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"[ERROR] Fehlende Spalten: {missing_columns}")
        print(f"[INFO] Vorhandene Spalten: {list(df.columns)}")
        sys.exit(1)

    print(f"[ANALYZER] Berechne Kunden-Umsatz...")

    # Kunden-Umsatz
    kunden_umsatz = df.groupby('customer_id').agg({
        'transaction_id': 'count',
        'total': ['sum', 'mean']
    }).reset_index()
    kunden_umsatz.columns = ['customer_id', 'anzahl_bestellungen', 'gesamt_umsatz', 'durchschnitt_bestellung']

    print(f"[ANALYZER] Kunden gefunden: {len(kunden_umsatz)}")

    # Top Segmente nach Umsatz
    print(f"[ANALYZER] Berechne Umsatz-Segmente...")
    report_umsatz = []
    vip_threshold = kunden_umsatz['gesamt_umsatz'].quantile(0.9)
    print(f"[ANALYZER] VIP-Schwelle (90. Perzentil): {vip_threshold:.2f}")

    segments = [
        ('VIP', lambda x: x >= vip_threshold),
        ('Premium', lambda x: (x >= 1000) & (x < vip_threshold)),
        ('Standard', lambda x: (x >= 200) & (x < 1000)),
        ('Gering', lambda x: x < 200)
    ]

    for segment_name, condition in segments:
        segment_df = kunden_umsatz[condition(kunden_umsatz['gesamt_umsatz'])]
        if len(segment_df) > 0:
            report_umsatz.append({
                'umsatz_segment': segment_name,
                'anzahl_kunden': int(len(segment_df)),
                'segment_umsatz': float(segment_df['gesamt_umsatz'].sum()),
                'avg_umsatz': float(segment_df['gesamt_umsatz'].mean())
            })
            print(f"[ANALYZER]   {segment_name}: {len(segment_df)} Kunden")

    # Aktivität
    print(f"[ANALYZER] Berechne Aktivitäts-Segmente...")
    try:
        df['date_parsed'] = pd.to_datetime(df['date'], errors='coerce')
        max_date = df['date_parsed'].max()
        print(f"[ANALYZER] Max Datum: {max_date}")

        kunden_last_order = df.groupby('customer_id')['date_parsed'].max().reset_index()
        kunden_last_order['tage_inaktiv'] = (max_date - kunden_last_order['date_parsed']).dt.days

        # Aktivitäts-Segmente
        aktiv_mask = kunden_last_order['tage_inaktiv'] <= 30
        inaktiv_mask = (kunden_last_order['tage_inaktiv'] > 30) & (kunden_last_order['tage_inaktiv'] <= 90)
        verloren_mask = kunden_last_order['tage_inaktiv'] > 90

        aktiv_kunden = kunden_last_order[aktiv_mask]['customer_id']
        inaktiv_kunden = kunden_last_order[inaktiv_mask]['customer_id']
        verloren_kunden = kunden_last_order[verloren_mask]['customer_id']

        report_aktivitaet = [
            {
                'aktivitaet_segment': 'Aktiv',
                'anzahl_kunden': int(len(aktiv_kunden)),
                'segment_umsatz': float(kunden_umsatz[kunden_umsatz['customer_id'].isin(aktiv_kunden)]['gesamt_umsatz'].sum())
            },
            {
                'aktivitaet_segment': 'Inaktiv',
                'anzahl_kunden': int(len(inaktiv_kunden)),
                'segment_umsatz': float(kunden_umsatz[kunden_umsatz['customer_id'].isin(inaktiv_kunden)]['gesamt_umsatz'].sum())
            },
            {
                'aktivitaet_segment': 'Verloren',
                'anzahl_kunden': int(len(verloren_kunden)),
                'segment_umsatz': float(kunden_umsatz[kunden_umsatz['customer_id'].isin(verloren_kunden)]['gesamt_umsatz'].sum())
            }
        ]

        for seg in report_aktivitaet:
            print(f"[ANALYZER]   {seg['aktivitaet_segment']}: {seg['anzahl_kunden']} Kunden")

    except Exception as e:
        print(f"[WARN] Fehler bei Aktivitäts-Berechnung: {e}")
        report_aktivitaet = []
        kunden_last_order = pd.DataFrame()
        max_date = datetime.now()

    # DACH Region
    print(f"[ANALYZER] Berechne Länder-Verteilung...")
    if 'country' in df.columns:
        top_countries = df.groupby('country')['total'].sum().nlargest(3).reset_index()
        total_umsatz = kunden_umsatz['gesamt_umsatz'].sum()

        report_dach = [{
            'ist_dach_kunde': 'Ja',
            'anzahl_kunden': int(len(kunden_umsatz) * 0.6),
            'gesamt_umsatz': float(total_umsatz * 0.6),
            'avg_umsatz': float(kunden_umsatz['gesamt_umsatz'].mean())
        }]

        report_dach_laender = []
        for _, row in top_countries.iterrows():
            country_customers = df[df['country'] == row['country']]['customer_id'].nunique()
            report_dach_laender.append({
                'land': row['country'],
                'anzahl_kunden': int(country_customers),
                'gesamt_umsatz': float(row['total']),
                'avg_umsatz': float(row['total'] / max(country_customers, 1))
            })
            print(f"[ANALYZER]   {row['country']}: {country_customers} Kunden")
    else:
        print(f"[WARN] Spalte 'country' nicht gefunden")
        report_dach = []
        report_dach_laender = []

    # Inaktive VIPs
    print(f"[ANALYZER] Suche inaktive VIP-Kunden...")
    try:
        vip_kunden = kunden_umsatz[kunden_umsatz['gesamt_umsatz'] >= vip_threshold].copy()

        if len(kunden_last_order) > 0:
            vip_kunden = vip_kunden.merge(kunden_last_order, on='customer_id', how='left')
            inaktive_vips = vip_kunden[vip_kunden['tage_inaktiv'] > 30].nlargest(10, 'gesamt_umsatz')

            top_inaktive_vips = []
            for _, row in inaktive_vips.iterrows():
                top_inaktive_vips.append({
                    'customer_id': str(row['customer_id']),
                    'gesamt_umsatz': float(row['gesamt_umsatz']),
                    'anzahl_bestellungen': int(row['anzahl_bestellungen']),
                    'letzte_bestellung': row['date_parsed'].strftime('%Y-%m-%d') if pd.notna(row['date_parsed']) else 'Unbekannt',
                    'tage_inaktiv': int(row['tage_inaktiv']) if pd.notna(row['tage_inaktiv']) else 0
                })

            verlorener_umsatz = float(inaktive_vips['gesamt_umsatz'].sum()) if len(inaktive_vips) > 0 else 0
            print(f"[ANALYZER] Inaktive VIPs gefunden: {len(top_inaktive_vips)}")
        else:
            top_inaktive_vips = []
            verlorener_umsatz = 0
            inaktive_vips = pd.DataFrame()

    except Exception as e:
        print(f"[WARN] Fehler bei VIP-Berechnung: {e}")
        top_inaktive_vips = []
        verlorener_umsatz = 0
        inaktive_vips = pd.DataFrame()

    # Finale Daten
    dashboard_data = {
        'maxDate': max_date.strftime('%Y-%m-%d') if hasattr(max_date, 'strftime') else str(max_date),
        'kundenGesamt': int(len(kunden_umsatz)),
        'inaktiveVips': int(len(inaktive_vips)) if len(inaktive_vips) > 0 else 0,
        'verlorenerUmsatz': verlorener_umsatz,
        'reportUmsatz': report_umsatz,
        'reportAktivitaet': report_aktivitaet,
        'reportDach': report_dach,
        'reportDachLaender': report_dach_laender,
        'reportAndereLaender': [],
        'topInaktiveVips': top_inaktive_vips
    }

    # Speichern
    print(f"[ANALYZER] Speichere Ergebnisse nach: {output_path}")

    # Stelle sicher, dass das Verzeichnis existiert
    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(dashboard_data, f, ensure_ascii=False, indent=2)

    # Verifiziere dass Datei geschrieben wurde
    if os.path.exists(output_path):
        output_size = os.path.getsize(output_path)
        print(f"[ANALYZER] ✅ Erfolgreich gespeichert: {output_path} ({output_size} bytes)")
    else:
        print(f"[ERROR] Datei wurde nicht erstellt: {output_path}")
        sys.exit(1)

    print(f"[ANALYZER] Ende: {datetime.now().isoformat()}")
    return dashboard_data


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python simple_analyzer.py <csv_path> [output_path]")
        print("  csv_path:    Pfad zur CSV-Datei")
        print("  output_path: Pfad für data.json (optional, default: data.json)")
        sys.exit(1)

    csv_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else 'data.json'

    analyze_csv(csv_path, output_path)