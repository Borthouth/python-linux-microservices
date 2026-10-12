#!/usr/bin/env python3

import sqlite3

METRICS_DB = "/var/lib/scanhub/metrics.db"

conn = sqlite3.connect(METRICS_DB)

conn.execute("""
    UPDATE metrics
    SET
        total_scans = 0,
        clean_scans = 0,
        infected_scans = 0,
        scan_errors = 0,
        total_bytes = 0,
        largest_file = 0,
        total_scan_ms = 0,
        total_upload_ms = 0
    WHERE id = 1
""")

conn.commit()
conn.close()

print("Metrics reset successfully.")
