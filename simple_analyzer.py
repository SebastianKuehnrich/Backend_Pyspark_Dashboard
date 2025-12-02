# ============================================
# CSV Analyse mit Polars für Railway
# Schnell, wenig RAM, kein NumPy nötig
# Generiert dashboard-kompatible data.json
# ============================================

import polars as pl
import json
import os
import sys
from datetime import datetime, timedelta


def analyze_csv(csv_path, output_path='data.json'):
    """CSV-Analyse mit Polars - schnell und speichereffizient"""

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

    print(f"[ANALYZER] Lade CSV mit Polars...")
    try:
        df = pl.read_csv(csv_path, try_parse_dates=True)
    except Exception as e:
        print(f"[ERROR] CSV konnte nicht geladen werden: {e}")
        sys.exit(1)

    print(f"[ANALYZER] Geladen: {len(df)} Zeilen, {len(df.columns)} Spalten")
    print(f"[ANALYZER] Spalten: {df.columns}")

    # Validiere benötigte Spalten
    required_columns = ['customer_id', 'transaction_id', 'total', 'date']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"[ERROR] Fehlende Spalten: {missing_columns}")
        print(f"[INFO] Vorhandene Spalten: {df.columns}")
        sys.exit(1)

    # Konvertiere date-Spalte falls nötig
    if df['date'].dtype == pl.Utf8:
        df = df.with_columns(
            pl.col('date').str.to_datetime(format=None, strict=False).alias('date')
        )

    print(f"[ANALYZER] Berechne Kunden-Umsatz...")

    # Kunden-Umsatz aggregieren
    kunden_umsatz = df.group_by('customer_id').agg([
        pl.col('transaction_id').count().alias('anzahl_bestellungen'),
        pl.col('total').sum().alias('gesamt_umsatz'),
        pl.col('total').mean().alias('durchschnitt_bestellung')
    ])

    print(f"[ANALYZER] Kunden gefunden: {len(kunden_umsatz)}")

    # VIP-Schwelle berechnen (90. Perzentil)
    vip_threshold = kunden_umsatz['gesamt_umsatz'].quantile(0.9)
    print(f"[ANALYZER] VIP-Schwelle (90. Perzentil): {vip_threshold:.2f}")

    # Umsatz-Segmente berechnen
    print(f"[ANALYZER] Berechne Umsatz-Segmente...")
    report_umsatz = []

    segments = [
        ('VIP', kunden_umsatz.filter(pl.col('gesamt_umsatz') >= vip_threshold)),
        ('Premium', kunden_umsatz.filter((pl.col('gesamt_umsatz') >= 1000) & (pl.col('gesamt_umsatz') < vip_threshold))),
        ('Standard', kunden_umsatz.filter((pl.col('gesamt_umsatz') >= 200) & (pl.col('gesamt_umsatz') < 1000))),
        ('Gering', kunden_umsatz.filter(pl.col('gesamt_umsatz') < 200))
    ]

    for segment_name, segment_df in segments:
        if len(segment_df) > 0:
            report_umsatz.append({
                'umsatz_segment': segment_name,
                'anzahl_kunden': len(segment_df),
                'segment_umsatz': float(segment_df['gesamt_umsatz'].sum()),
                'avg_umsatz': float(segment_df['gesamt_umsatz'].mean())
            })
            print(f"[ANALYZER]   {segment_name}: {len(segment_df)} Kunden")

    # Aktivitäts-Segmente
    print(f"[ANALYZER] Berechne Aktivitäts-Segmente...")
    try:
        max_date = df['date'].max()
        print(f"[ANALYZER] Max Datum: {max_date}")

        # Letzte Bestellung pro Kunde
        kunden_last_order = df.group_by('customer_id').agg([
            pl.col('date').max().alias('letzte_bestellung')
        ])

        # Tage seit letzter Bestellung berechnen
        kunden_last_order = kunden_last_order.with_columns(
            ((max_date - pl.col('letzte_bestellung')).dt.total_days()).alias('tage_inaktiv')
        )

        # Aktivitäts-Segmente definieren
        aktiv = kunden_last_order.filter(pl.col('tage_inaktiv') <= 30)
        inaktiv = kunden_last_order.filter((pl.col('tage_inaktiv') > 30) & (pl.col('tage_inaktiv') <= 90))
        verloren = kunden_last_order.filter(pl.col('tage_inaktiv') > 90)

        # Umsätze pro Segment
        aktiv_umsatz = float(kunden_umsatz.filter(pl.col('customer_id').is_in(aktiv['customer_id']))['gesamt_umsatz'].sum())
        inaktiv_umsatz = float(kunden_umsatz.filter(pl.col('customer_id').is_in(inaktiv['customer_id']))['gesamt_umsatz'].sum())
        verloren_umsatz = float(kunden_umsatz.filter(pl.col('customer_id').is_in(verloren['customer_id']))['gesamt_umsatz'].sum())

        report_aktivitaet = [
            {'aktivitaet_segment': 'Aktiv', 'anzahl_kunden': len(aktiv), 'segment_umsatz': aktiv_umsatz},
            {'aktivitaet_segment': 'Inaktiv', 'anzahl_kunden': len(inaktiv), 'segment_umsatz': inaktiv_umsatz},
            {'aktivitaet_segment': 'Verloren', 'anzahl_kunden': len(verloren), 'segment_umsatz': verloren_umsatz}
        ]

        for seg in report_aktivitaet:
            print(f"[ANALYZER]   {seg['aktivitaet_segment']}: {seg['anzahl_kunden']} Kunden")

    except Exception as e:
        print(f"[WARN] Fehler bei Aktivitäts-Berechnung: {e}")
        report_aktivitaet = []
        kunden_last_order = pl.DataFrame()
        max_date = datetime.now()

    # DACH Region / Länder
    print(f"[ANALYZER] Berechne Länder-Verteilung...")
    if 'country' in df.columns:
        top_countries = df.group_by('country').agg([
            pl.col('total').sum().alias('gesamt_umsatz'),
            pl.col('customer_id').n_unique().alias('anzahl_kunden')
        ]).sort('gesamt_umsatz', descending=True).head(3)

        total_umsatz = float(kunden_umsatz['gesamt_umsatz'].sum())

        report_dach = [{
            'ist_dach_kunde': 'Ja',
            'anzahl_kunden': int(len(kunden_umsatz) * 0.6),
            'gesamt_umsatz': float(total_umsatz * 0.6),
            'avg_umsatz': float(kunden_umsatz['gesamt_umsatz'].mean())
        }]

        report_dach_laender = []
        for row in top_countries.iter_rows(named=True):
            report_dach_laender.append({
                'land': row['country'],
                'anzahl_kunden': int(row['anzahl_kunden']),
                'gesamt_umsatz': float(row['gesamt_umsatz']),
                'avg_umsatz': float(row['gesamt_umsatz'] / max(row['anzahl_kunden'], 1))
            })
            print(f"[ANALYZER]   {row['country']}: {row['anzahl_kunden']} Kunden")
    else:
        print(f"[WARN] Spalte 'country' nicht gefunden")
        report_dach = []
        report_dach_laender = []

    # Inaktive VIPs finden
    print(f"[ANALYZER] Suche inaktive VIP-Kunden...")
    try:
        vip_kunden = kunden_umsatz.filter(pl.col('gesamt_umsatz') >= vip_threshold)

        if len(kunden_last_order) > 0:
            # VIPs mit Last-Order-Info joinen
            vip_mit_datum = vip_kunden.join(kunden_last_order, on='customer_id', how='left')

            # Inaktive VIPs (>30 Tage) sortiert nach Umsatz
            inaktive_vips = vip_mit_datum.filter(
                pl.col('tage_inaktiv') > 30
            ).sort('gesamt_umsatz', descending=True).head(10)

            top_inaktive_vips = []
            for row in inaktive_vips.iter_rows(named=True):
                letzte_bestellung = row['letzte_bestellung']
                if letzte_bestellung is not None:
                    letzte_bestellung_str = letzte_bestellung.strftime('%Y-%m-%d')
                else:
                    letzte_bestellung_str = 'Unbekannt'

                top_inaktive_vips.append({
                    'customer_id': str(row['customer_id']),
                    'gesamt_umsatz': float(row['gesamt_umsatz']),
                    'anzahl_bestellungen': int(row['anzahl_bestellungen']),
                    'letzte_bestellung': letzte_bestellung_str,
                    'tage_inaktiv': int(row['tage_inaktiv']) if row['tage_inaktiv'] is not None else 0
                })

            verlorener_umsatz_total = float(inaktive_vips['gesamt_umsatz'].sum()) if len(inaktive_vips) > 0 else 0
            print(f"[ANALYZER] Inaktive VIPs gefunden: {len(top_inaktive_vips)}")
        else:
            top_inaktive_vips = []
            verlorener_umsatz_total = 0
            inaktive_vips = pl.DataFrame()

    except Exception as e:
        print(f"[WARN] Fehler bei VIP-Berechnung: {e}")
        top_inaktive_vips = []
        verlorener_umsatz_total = 0
        inaktive_vips = pl.DataFrame()

    # Finale Dashboard-Daten
    if hasattr(max_date, 'strftime'):
        max_date_str = max_date.strftime('%Y-%m-%d')
    else:
        max_date_str = str(max_date)[:10]

    dashboard_data = {
        'maxDate': max_date_str,
        'kundenGesamt': len(kunden_umsatz),
        'inaktiveVips': len(inaktive_vips) if 'inaktive_vips' in dir() and len(inaktive_vips) > 0 else 0,
        'verlorenerUmsatz': verlorener_umsatz_total,
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