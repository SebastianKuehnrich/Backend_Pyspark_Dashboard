# ============================================
# CSV Analyse mit Polars für Railway
# Exakt angepasst an Dashboard-Struktur
# ============================================

import polars as pl
import json
import os
import sys
from datetime import datetime

# DACH-Länder Definition
DACH_COUNTRIES = ["Germany", "Austria", "Switzerland"]


def analyze_csv(csv_path, output_path='data.json'):
    """CSV-Analyse mit Polars - exakt wie PySpark Original"""

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
        sys.exit(1)

    # Konvertiere date-Spalte falls nötig
    if df['date'].dtype == pl.Utf8:
        df = df.with_columns(
            pl.col('date').str.to_datetime(format=None, strict=False).alias('date')
        )

    # ============================================
    # KUNDEN-UMSATZ AGGREGIEREN
    # ============================================
    print(f"[ANALYZER] Berechne Kunden-Umsatz...")

    kunden_umsatz = df.group_by('customer_id').agg([
        pl.col('transaction_id').count().alias('anzahl_bestellungen'),
        pl.col('total').sum().alias('gesamt_umsatz'),
        pl.col('total').mean().alias('durchschnitt_bestellung')
    ])

    kunden_gesamt = len(kunden_umsatz)
    print(f"[ANALYZER] Kunden gefunden: {kunden_gesamt}")

    # ============================================
    # UMSATZ-SEGMENTE (Perzentil-basiert wie Original PySpark)
    # ============================================
    print(f"[ANALYZER] Berechne Umsatz-Segmente...")

    # VIP-Schwelle = 90. Perzentil (Top 10% der Kunden)
    vip_threshold = kunden_umsatz['gesamt_umsatz'].quantile(0.9)
    print(f"[ANALYZER] VIP-Schwelle (90. Perzentil): {vip_threshold:,.2f} EUR")

    # Segmente basierend auf Perzentilen
    vip_df = kunden_umsatz.filter(pl.col('gesamt_umsatz') >= vip_threshold)
    premium_df = kunden_umsatz.filter((pl.col('gesamt_umsatz') >= 1000) & (pl.col('gesamt_umsatz') < vip_threshold))
    standard_df = kunden_umsatz.filter((pl.col('gesamt_umsatz') >= 200) & (pl.col('gesamt_umsatz') < 1000))
    gering_df = kunden_umsatz.filter(pl.col('gesamt_umsatz') < 200)

    # Gesamtumsatz für Prozentberechnung
    total_umsatz = float(kunden_umsatz['gesamt_umsatz'].sum())

    report_umsatz = []
    for segment_name, segment_df in [('VIP', vip_df), ('Premium', premium_df), ('Standard', standard_df), ('Gering', gering_df)]:
        # Alle Segmente anzeigen, auch wenn leer
        if len(segment_df) > 0:
            segment_umsatz = float(segment_df['gesamt_umsatz'].sum())
            avg_umsatz = float(segment_df['gesamt_umsatz'].mean())
        else:
            segment_umsatz = 0.0
            avg_umsatz = 0.0

        report_umsatz.append({
            'umsatz_segment': segment_name,
            'anzahl_kunden': float(len(segment_df)),
            'segment_umsatz': round(segment_umsatz, 2),
            'avg_umsatz': round(avg_umsatz, 2),
            'umsatz_anteil_prozent': round((segment_umsatz / total_umsatz) * 100, 2) if total_umsatz > 0 else 0
        })
        print(f"[ANALYZER]   {segment_name}: {len(segment_df)} Kunden, {segment_umsatz:,.2f} EUR")

    # Nach Umsatz sortieren (absteigend)
    report_umsatz.sort(key=lambda x: x['segment_umsatz'], reverse=True)

    # ============================================
    # AKTIVITÄTS-SEGMENTE
    # ============================================
    print(f"[ANALYZER] Berechne Aktivitäts-Segmente...")

    try:
        max_date = df['date'].max()
        print(f"[ANALYZER] Max Datum: {max_date}")

        # Letzte Bestellung pro Kunde
        kunden_last_order = df.group_by('customer_id').agg([
            pl.col('date').max().alias('letzte_bestellung')
        ])

        # Tage seit letzter Bestellung
        kunden_last_order = kunden_last_order.with_columns(
            ((max_date - pl.col('letzte_bestellung')).dt.total_days()).alias('tage_inaktiv')
        )

        # Segmente
        aktiv = kunden_last_order.filter(pl.col('tage_inaktiv') <= 30)
        inaktiv = kunden_last_order.filter((pl.col('tage_inaktiv') > 30) & (pl.col('tage_inaktiv') <= 90))
        verloren = kunden_last_order.filter(pl.col('tage_inaktiv') > 90)

        # Umsätze berechnen
        aktiv_umsatz = float(kunden_umsatz.filter(pl.col('customer_id').is_in(aktiv['customer_id']))['gesamt_umsatz'].sum())
        inaktiv_umsatz = float(kunden_umsatz.filter(pl.col('customer_id').is_in(inaktiv['customer_id']))['gesamt_umsatz'].sum())
        verloren_umsatz = float(kunden_umsatz.filter(pl.col('customer_id').is_in(verloren['customer_id']))['gesamt_umsatz'].sum())

        aktiv_count = len(aktiv)
        inaktiv_count = len(inaktiv)
        verloren_count = len(verloren)

        report_aktivitaet = [
            {
                'aktivitaet_segment': 'Aktiv',
                'anzahl_kunden': float(aktiv_count),
                'segment_umsatz': round(aktiv_umsatz, 2),
                'avg_umsatz': round(aktiv_umsatz / aktiv_count, 2) if aktiv_count > 0 else 0
            },
            {
                'aktivitaet_segment': 'Inaktiv',
                'anzahl_kunden': float(inaktiv_count),
                'segment_umsatz': round(inaktiv_umsatz, 2),
                'avg_umsatz': round(inaktiv_umsatz / inaktiv_count, 2) if inaktiv_count > 0 else 0
            },
            {
                'aktivitaet_segment': 'Verloren',
                'anzahl_kunden': float(verloren_count),
                'segment_umsatz': round(verloren_umsatz, 2),
                'avg_umsatz': round(verloren_umsatz / verloren_count, 2) if verloren_count > 0 else 0
            }
        ]

        # Nach Umsatz sortieren
        report_aktivitaet.sort(key=lambda x: x['segment_umsatz'], reverse=True)

        for seg in report_aktivitaet:
            print(f"[ANALYZER]   {seg['aktivitaet_segment']}: {int(seg['anzahl_kunden'])} Kunden")

    except Exception as e:
        print(f"[WARN] Fehler bei Aktivitäts-Berechnung: {e}")
        report_aktivitaet = []
        kunden_last_order = pl.DataFrame()
        max_date = datetime.now()

    # ============================================
    # DACH-ANALYSE (Germany, Austria, Switzerland)
    # ============================================
    print(f"[ANALYZER] Berechne DACH-Analyse...")

    report_dach = []
    report_dach_laender = []
    report_andere_laender = []

    if 'country' in df.columns:
        # Prüfe welche Kunden DACH-Bestellungen haben
        dach_bestellungen = df.filter(pl.col('country').is_in(DACH_COUNTRIES))
        dach_kunden_ids = dach_bestellungen['customer_id'].unique()

        # DACH-Kunden Statistiken
        dach_kunden = kunden_umsatz.filter(pl.col('customer_id').is_in(dach_kunden_ids))
        nicht_dach_kunden = kunden_umsatz.filter(~pl.col('customer_id').is_in(dach_kunden_ids))

        # Report DACH ja/nein
        if len(dach_kunden) > 0:
            report_dach.append({
                'ist_dach_kunde': 'Ja',
                'anzahl_kunden': float(len(dach_kunden)),
                'gesamt_umsatz': round(float(dach_kunden['gesamt_umsatz'].sum()), 2),
                'avg_umsatz': round(float(dach_kunden['gesamt_umsatz'].mean()), 2)
            })

        if len(nicht_dach_kunden) > 0:
            report_dach.append({
                'ist_dach_kunde': 'Nein',
                'anzahl_kunden': float(len(nicht_dach_kunden)),
                'gesamt_umsatz': round(float(nicht_dach_kunden['gesamt_umsatz'].sum()), 2),
                'avg_umsatz': round(float(nicht_dach_kunden['gesamt_umsatz'].mean()), 2)
            })

        # Länder-Details
        laender_stats = df.group_by('country').agg([
            pl.col('transaction_id').count().alias('anzahl_bestellungen'),
            pl.col('total').sum().alias('gesamt_umsatz'),
            pl.col('total').mean().alias('avg_bestellung')
        ]).sort('gesamt_umsatz', descending=True)

        for row in laender_stats.iter_rows(named=True):
            land_data = {
                'country': row['country'],
                'anzahl_bestellungen': float(row['anzahl_bestellungen']),
                'gesamt_umsatz': round(float(row['gesamt_umsatz']), 2),
                'avg_bestellung': round(float(row['avg_bestellung']), 2)
            }

            if row['country'] in DACH_COUNTRIES:
                report_dach_laender.append(land_data)
                print(f"[ANALYZER]   DACH - {row['country']}: {row['anzahl_bestellungen']} Bestellungen")
            else:
                report_andere_laender.append(land_data)
                print(f"[ANALYZER]   Andere - {row['country']}: {row['anzahl_bestellungen']} Bestellungen")

    else:
        print(f"[WARN] Spalte 'country' nicht gefunden - DACH-Analyse übersprungen")

    # ============================================
    # INAKTIVE VIP-KUNDEN (> 30 Tage inaktiv)
    # ============================================
    print(f"[ANALYZER] Suche inaktive VIP-Kunden...")

    top_inaktive_vips = []
    verlorener_umsatz_total = 0
    inaktive_vips_count = 0

    try:
        # VIPs = Top 10% (90. Perzentil) - gleiche Schwelle wie oben
        vip_kunden = kunden_umsatz.filter(pl.col('gesamt_umsatz') >= vip_threshold)

        if len(kunden_last_order) > 0 and len(vip_kunden) > 0:
            # VIPs mit Inaktivitäts-Info joinen
            vip_mit_datum = vip_kunden.join(kunden_last_order, on='customer_id', how='left')

            # Inaktive VIPs (> 30 Tage) - TOP 30
            inaktive_vips = vip_mit_datum.filter(
                pl.col('tage_inaktiv') > 30
            ).sort('gesamt_umsatz', descending=True).head(30)

            inaktive_vips_count = len(vip_mit_datum.filter(pl.col('tage_inaktiv') > 30))
            verlorener_umsatz_total = float(vip_mit_datum.filter(pl.col('tage_inaktiv') > 30)['gesamt_umsatz'].sum())

            for row in inaktive_vips.iter_rows(named=True):
                letzte_bestellung = row['letzte_bestellung']
                if letzte_bestellung is not None:
                    letzte_bestellung_str = letzte_bestellung.strftime('%Y-%m-%d')
                else:
                    letzte_bestellung_str = 'Unbekannt'

                top_inaktive_vips.append({
                    'customer_id': float(row['customer_id']),
                    'gesamt_umsatz': round(float(row['gesamt_umsatz']), 2),
                    'anzahl_bestellungen': float(row['anzahl_bestellungen']),
                    'letzte_bestellung': letzte_bestellung_str,
                    'tage_inaktiv': float(row['tage_inaktiv']) if row['tage_inaktiv'] is not None else 0
                })

            print(f"[ANALYZER] Inaktive VIPs gefunden: {inaktive_vips_count}")
            print(f"[ANALYZER] Verlorener Umsatz: {verlorener_umsatz_total:,.2f} EUR")

    except Exception as e:
        print(f"[WARN] Fehler bei VIP-Berechnung: {e}")

    # ============================================
    # DASHBOARD-DATEN ZUSAMMENSTELLEN
    # ============================================

    if hasattr(max_date, 'strftime'):
        max_date_str = max_date.strftime('%Y-%m-%d')
    else:
        max_date_str = str(max_date)[:10]

    dashboard_data = {
        'maxDate': max_date_str,
        'kundenGesamt': kunden_gesamt,
        'inaktiveVips': inaktive_vips_count,
        'verlorenerUmsatz': round(verlorener_umsatz_total, 2),
        'reportUmsatz': report_umsatz,
        'reportAktivitaet': report_aktivitaet,
        'reportDach': report_dach,
        'reportDachLaender': report_dach_laender,
        'reportAndereLaender': report_andere_laender,
        'topInaktiveVips': top_inaktive_vips
    }

    # ============================================
    # SPEICHERN
    # ============================================
    print(f"[ANALYZER] Speichere Ergebnisse nach: {output_path}")

    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(dashboard_data, f, ensure_ascii=False, indent=2)

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
        sys.exit(1)

    csv_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else 'data.json'

    analyze_csv(csv_path, output_path)