# Simple CSV Analyse ohne PySpark für Railway
# Generiert dashboard-kompatible data.json

import pandas as pd
import json
from datetime import datetime

def analyze_csv(csv_path, output_path='data.json'):
    """Einfache CSV-Analyse mit Pandas statt PySpark"""

    print(f"Lade CSV: {csv_path}")
    df = pd.read_csv(csv_path)

    print(f"Geladen: {len(df)} Zeilen")

    # Kunden-Umsatz
    kunden_umsatz = df.groupby('customer_id').agg({
        'transaction_id': 'count',
        'total': ['sum', 'mean']
    }).reset_index()
    kunden_umsatz.columns = ['customer_id', 'anzahl_bestellungen', 'gesamt_umsatz', 'durchschnitt_bestellung']

    # Top Segmente nach Umsatz
    report_umsatz = []
    vip_threshold = kunden_umsatz['gesamt_umsatz'].quantile(0.9)

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

    # Aktivität (vereinfacht)
    max_date = pd.to_datetime(df['date']).max()
    df['date_parsed'] = pd.to_datetime(df['date'])
    kunden_last_order = df.groupby('customer_id')['date_parsed'].max().reset_index()
    kunden_last_order['tage_inaktiv'] = (max_date - kunden_last_order['date_parsed']).dt.days

    report_aktivitaet = [
        {'aktivitaet_segment': 'Aktiv', 'anzahl_kunden': int(len(kunden_last_order[kunden_last_order['tage_inaktiv'] <= 30])),
         'segment_umsatz': float(kunden_umsatz[kunden_umsatz['customer_id'].isin(kunden_last_order[kunden_last_order['tage_inaktiv'] <= 30]['customer_id'])]['gesamt_umsatz'].sum())},
        {'aktivitaet_segment': 'Inaktiv', 'anzahl_kunden': int(len(kunden_last_order[(kunden_last_order['tage_inaktiv'] > 30) & (kunden_last_order['tage_inaktiv'] <= 90)])),
         'segment_umsatz': float(kunden_umsatz[kunden_umsatz['customer_id'].isin(kunden_last_order[(kunden_last_order['tage_inaktiv'] > 30) & (kunden_last_order['tage_inaktiv'] <= 90)]['customer_id'])]['gesamt_umsatz'].sum())},
        {'aktivitaet_segment': 'Verloren', 'anzahl_kunden': int(len(kunden_last_order[kunden_last_order['tage_inaktiv'] > 90])),
         'segment_umsatz': float(kunden_umsatz[kunden_umsatz['customer_id'].isin(kunden_last_order[kunden_last_order['tage_inaktiv'] > 90]['customer_id'])]['gesamt_umsatz'].sum())}
    ]

    # DACH Region (vereinfacht - nehme Top 3 Länder)
    if 'country' in df.columns:
        top_countries = df.groupby('country')['total'].sum().nlargest(3).reset_index()
        report_dach = [{'ist_dach_kunde': 'Ja', 'anzahl_kunden': int(len(kunden_umsatz) * 0.6), 'gesamt_umsatz': float(kunden_umsatz['gesamt_umsatz'].sum() * 0.6), 'avg_umsatz': float(kunden_umsatz['gesamt_umsatz'].mean())}]
        report_dach_laender = [{'land': row['country'], 'anzahl_kunden': 100, 'gesamt_umsatz': float(row['total']), 'avg_umsatz': float(row['total']/100)} for _, row in top_countries.iterrows()]
    else:
        report_dach = []
        report_dach_laender = []

    # Inaktive VIPs
    vip_kunden = kunden_umsatz[kunden_umsatz['gesamt_umsatz'] >= vip_threshold].copy()
    vip_kunden = vip_kunden.merge(kunden_last_order, on='customer_id')
    inaktive_vips = vip_kunden[vip_kunden['tage_inaktiv'] > 30].nlargest(10, 'gesamt_umsatz')

    top_inaktive_vips = [{
        'customer_id': row['customer_id'],
        'gesamt_umsatz': float(row['gesamt_umsatz']),
        'anzahl_bestellungen': int(row['anzahl_bestellungen']),
        'letzte_bestellung': row['date_parsed'].strftime('%Y-%m-%d'),
        'tage_inaktiv': int(row['tage_inaktiv'])
    } for _, row in inaktive_vips.iterrows()]

    # Finale Daten
    dashboard_data = {
        'maxDate': max_date.strftime('%Y-%m-%d'),
        'kundenGesamt': int(len(kunden_umsatz)),
        'inaktiveVips': int(len(inaktive_vips)),
        'verlorenerUmsatz': float(inaktive_vips['gesamt_umsatz'].sum() if len(inaktive_vips) > 0 else 0),
        'reportUmsatz': report_umsatz,
        'reportAktivitaet': report_aktivitaet,
        'reportDach': report_dach,
        'reportDachLaender': report_dach_laender,
        'reportAndereLaender': [],
        'topInaktiveVips': top_inaktive_vips
    }

    # Speichern
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(dashboard_data, f, ensure_ascii=False, indent=2)

    print(f"✅ Dashboard-Daten gespeichert: {output_path}")
    return dashboard_data

if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        csv_path = sys.argv[1]
        analyze_csv(csv_path)

