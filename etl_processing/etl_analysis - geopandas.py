"""
===============================================================================
Script Name:       Parcel QA/QC ETL with Geopandas
Author:            Andrew Sheppard
Role:              GIS Solutions Engineer
Email:             andrewsheppard8@gmail.com
Date Created:      2026-04-01
Last Updated:      2026-04-01

Purpose:
--------
This script demonstrates using Python (pandas and geopandas) for GIS data
processing and QA/QC workflows. It represents a complete ETL (Extract-Transform-Load)
workflow for parcel data, including validation, issue reporting, and clean/dirty
dataset generation.

Key Features:
-------------
    - Reads a parcel shapefile into a GeoDataFrame
    - Inspects schema and exports column types for audit
    - Flags data quality issues such as:
        - Missing or empty geometry
        - Duplicate APNs
        - Invalid or missing APNs
        - Invalid acreage
        - Parcels out-of-date based on DATE_ADDED
    - Aggregates issue counts per parcel and exports a detailed report
    - Generates summary metrics of dataset quality
    - Produces a clean dataset (no issues) and a "dirty" dataset (only issues)
    - Creates a log file recording ETL steps

Intended Audience:
-----------------
GIS analysts, developers, or portfolio reviewers interested in Python-based QA/QC
workflows in spatial datasets.

Usage:
------
Adjust the input/output paths as needed. Run in an environment with:
    - Python 3.x
    - pandas
    - geopandas
    - logging

Outputs:
--------
- CSV: schema_report.csv
- CSV: data_quality_issues.csv
- CSV: etl_summary_report.csv
- TXT: etl_log.txt
- Shapefiles: clean_parcels.shp, dirty_parcels.shp
===============================================================================
"""
import geopandas as gpd
import pandas as pd
import os
import datetime
import logging

# --- PATHS SETUP ---
# Base directory: two levels up from the script (repo root)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Input and output paths
parcels_path = os.path.join(BASE_DIR, "data", "raw", "Parcels.shp")
output_folder = os.path.join(BASE_DIR, "data", "output")
output_reports = os.path.join(output_folder, "reports")
output_shapefiles = os.path.join(output_folder, "shapefiles")

# Ensure output directories exist
os.makedirs(output_folder, exist_ok=True)
os.makedirs(output_reports, exist_ok=True)
os.makedirs(output_shapefiles, exist_ok=True)

# --- EXTRACT: LOAD PARCEL DATA ---
parcels = gpd.read_file(parcels_path)

# --- INSPECT SCHEMA & EXPORT ---
schema_output = os.path.join(output_reports, "schema_report.csv")
parcels.dtypes.to_csv(schema_output)
print("Output schema report for audit")

# --- TRANSFORM: DATA QUALITY FLAGS ---

total_parcels = len(parcels)  # Total number of parcels in dataset

# Identify missing geometry
missing_geom_mask = parcels['geometry'].isna()

# Identify duplicate APNs (all instances, not just first)
duplicate_mask = parcels.duplicated(subset=['APN'], keep=False)

# Identify invalid APNs (null or empty)
invalid_apn_mask = parcels['APN'].isna() | (parcels['APN'].str.strip() == "")

# Identify invalid or missing acreage
invalid_acreage_mask = parcels['ACREAGE'] <= 0

# Identify parcels out-of-date (> 1 year since DATE_ADDED)
parcels['DATE_ADDED'] = pd.to_datetime(parcels['DATE_ADDED'], errors='coerce')
stale_days = 365
date_threshold = datetime.datetime.now() - datetime.timedelta(days=stale_days)
date_mask = (parcels['DATE_ADDED'] < date_threshold) | (parcels['DATE_ADDED'].isna())

# Geometry validation
invalid_geom_mask = ~parcels.is_valid          # Invalid geometries
empty_geom_mask = parcels.geometry.is_empty    # Empty geometries

# --- AGGREGATE ISSUES PER PARCEL ---
issues_df = parcels[['APN']].copy()
issues_df['MISSING_GEOMETRY'] = missing_geom_mask
issues_df['DUPLICATE_APN'] = duplicate_mask
issues_df['OUT_OF_DATE'] = date_mask
issues_df['INVALID_APN'] = invalid_apn_mask
issues_df['INVALID_ACREAGE'] = invalid_acreage_mask
issues_df['INVALID_GEOMETRY'] = invalid_geom_mask
issues_df['EMPTY_GEOMETRY'] = empty_geom_mask

# Keep only rows with at least one issue
issues_df = issues_df[
    issues_df[['MISSING_GEOMETRY', 'DUPLICATE_APN', 'OUT_OF_DATE', 
               'INVALID_APN', 'INVALID_ACREAGE', 'INVALID_GEOMETRY', 
               'EMPTY_GEOMETRY']].any(axis=1)
]

# Add total issue count per parcel
issues_df['ISSUE_COUNT'] = issues_df[['MISSING_GEOMETRY', 'DUPLICATE_APN', 'OUT_OF_DATE',
                                      'INVALID_APN', 'INVALID_ACREAGE', 'INVALID_GEOMETRY', 
                                      'EMPTY_GEOMETRY']].sum(axis=1)

# Sort by parcels with the most issues first
issues_df = issues_df.sort_values(by='ISSUE_COUNT', ascending=False)

# --- LOAD: EXPORT DATA QUALITY TABLE ---
issues_output_path = os.path.join(output_reports, "data_quality_issues.csv")
issues_df.to_csv(issues_output_path, index=False)
print(f"Exported data quality issues → {issues_output_path}")

# --- SUMMARY REPORT ---
summary_df = pd.DataFrame({
    "Metric": [
        "Total Parcels",
        "Missing Geometry",
        "Unique Duplicate APNs",
        "Out of Date Parcel",
        "Invalid APNs",
        "Invalid Acreage",
        "Invalid Geometry",
        "Empty Geometry"
    ],
    "Value": [
        total_parcels,
        missing_geom_mask.sum(),
        parcels.loc[duplicate_mask, 'APN'].nunique(),
        date_mask.sum(),
        invalid_apn_mask.sum(),
        invalid_acreage_mask.sum(),
        invalid_geom_mask.sum(),
        empty_geom_mask.sum()
    ]
})
summary_output_path = os.path.join(output_reports, "etl_summary_report.csv")
summary_df.to_csv(summary_output_path, index=False)
print(f"Exported summary report → {summary_output_path}")

# --- LOG EXPORT ---
log_path = os.path.join(output_reports, "etl_log.txt")
logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logging.info("ETL process started")
for idx, row in summary_df.iterrows():
    logging.info(f"{row['Metric']}: {row['Value']}")
print(f"Exported log → {log_path}")

# --- EXPORT CLEAN PARCELS (NO ISSUES) ---
clean_parcels = parcels[~(
    missing_geom_mask |
    duplicate_mask |
    invalid_apn_mask |
    date_mask |
    invalid_geom_mask |
    empty_geom_mask
)]
clean_output_path = os.path.join(output_shapefiles, "clean_parcels.shp")
clean_parcels.to_file(clean_output_path)
print("Output shapefile with only clean parcels")

# --- EXPORT DIRTY PARCELS (ONLY ISSUES) ---
dirty_parcels = parcels[
    missing_geom_mask |
    duplicate_mask |
    invalid_apn_mask |
    date_mask |
    invalid_geom_mask |
    empty_geom_mask
]
dirty_output_path = os.path.join(output_shapefiles, "dirty_parcels.shp")
dirty_parcels.to_file(dirty_output_path)
print("Output shapefile with only dirty parcels")
