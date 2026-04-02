"""
===============================================================================
Script Name:       Parcel QA/QC ETL with ArcPy
Author:            Andrew Sheppard
Role:              GIS Solutions Engineer
Email:             andrewsheppard8@gmail.com
Date Created:      2026-04-01
Last Updated:      2026-04-01

Purpose:
--------
This script demonstrates using Python with ArcPy (and GeoPandas for preprocessing)
to perform a complete ETL (Extract-Transform-Load) workflow for parcel datasets.
It handles spatial and attribute validation, generates QA/QC reports, and exports
clean and "dirty" datasets to a file geodatabase with proper field types and 
spatial indexing.

Key Features:
-------------
    - Reads a parcel shapefile and inspects its schema for auditing
    - Flags data quality issues including:
        - Missing or empty geometry
        - Duplicate APNs
        - Invalid or missing APNs
        - Invalid acreage
        - Out-of-date parcels based on DATE_ADDED
    - Aggregates issue counts per parcel and exports detailed CSV reports
    - Generates a summary metrics report for overall dataset quality
    - Produces:
        - A clean parcel dataset (no issues)
        - A dirty parcel dataset (only issues)
        - The original parcel dataset
      All exported to a geodatabase with spatial indexes
    - Converts string DATE_ADDED fields to proper Date fields in the geodatabase
    - Maintains a log file of ETL process steps for traceability

Intended Audience:
-----------------
GIS analysts, developers, and portfolio reviewers interested in ArcGIS Python 
automation, QA/QC workflows, and geodatabase management.

Usage:
------
Adjust input/output paths as needed. Run in an environment with:
    - Python 3.x
    - ArcGIS Pro / ArcPy
    - geopandas, pandas
    - logging
    
Outputs:
--------
- File Geodatabase: original_parcels, clean_parcels, dirty_parcels (with spatial indexes)
- CSV: schema_report.csv, data_quality_issues.csv, etl_summary_report.csv
- TXT: etl_log.txt
===============================================================================
"""
import geopandas as gpd
import pandas as pd
import os
import datetime
import logging
import arcpy
import glob

# --- PATHS ---
# Base directory: two levels up from the script (repo root)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Input and output paths
parcels_path = os.path.join(BASE_DIR, "data", "raw", "Parcels.shp")
output_folder = os.path.join(BASE_DIR, "data", "output")
output_reports = os.path.join(output_folder, "reports")
output_gdb_folder = os.path.join(output_folder, "gdb")
gdb_name = "etl_output.gdb"
gdb_path = os.path.join(output_gdb_folder, gdb_name)

# Ensure required folders exist
os.makedirs(output_folder, exist_ok=True)
os.makedirs(output_reports, exist_ok=True)
os.makedirs(output_gdb_folder, exist_ok=True)

# --- LOGGING SETUP ---
log_path = os.path.join(output_reports, "etl_log.txt")
logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logging.info("ETL process started")
print("ETL process started")

# --- EXTRACT: LOAD PARCELS ---
parcels = gpd.read_file(parcels_path)
total_parcels = len(parcels)

# --- SCHEMA REPORT ---
# Export column types to CSV for audit and ETL validation
schema_output = os.path.join(output_reports, "schema_report.csv")
parcels.dtypes.to_csv(schema_output)
logging.info("Exported schema report")
print("Output schema report")

# --- TRANSFORM: DATA QUALITY FLAGS ---
# Identify common QA/QC issues for parcels
missing_geom_mask = parcels['geometry'].isna()  # Missing geometry
duplicate_mask = parcels.duplicated(subset=['APN'], keep=False)  # Duplicate APNs
invalid_apn_mask = parcels['APN'].isna() | (parcels['APN'].str.strip() == "")  # Null/empty APNs
invalid_acreage_mask = parcels['ACREAGE'] <= 0  # Non-positive acreage

# Identify parcels out-of-date based on DATE_ADDED
parcels['DATE_ADDED'] = pd.to_datetime(parcels['DATE_ADDED'], errors='coerce')
stale_days = 365
date_threshold = datetime.datetime.now() - datetime.timedelta(days=stale_days)
date_mask = (parcels['DATE_ADDED'] < date_threshold) | (parcels['DATE_ADDED'].isna())

# Geometry validation
invalid_geom_mask = ~parcels.is_valid        # Invalid geometries
empty_geom_mask = parcels.geometry.is_empty  # Empty geometries

# --- AGGREGATE ISSUES ---
issues_df = parcels[['APN']].copy()
issues_df['MISSING_GEOMETRY'] = missing_geom_mask
issues_df['DUPLICATE_APN'] = duplicate_mask
issues_df['OUT_OF_DATE'] = date_mask
issues_df['INVALID_APN'] = invalid_apn_mask
issues_df['INVALID_ACREAGE'] = invalid_acreage_mask
issues_df['INVALID_GEOMETRY'] = invalid_geom_mask
issues_df['EMPTY_GEOMETRY'] = empty_geom_mask

# Keep only parcels with at least one issue
issues_df = issues_df[
    missing_geom_mask | duplicate_mask | date_mask | invalid_apn_mask |
    invalid_acreage_mask | invalid_geom_mask | empty_geom_mask
]

# Count total issues per parcel
issues_df['ISSUE_COUNT'] = (
    issues_df[['MISSING_GEOMETRY', 'DUPLICATE_APN', 'OUT_OF_DATE',
               'INVALID_APN', 'INVALID_ACREAGE', 'INVALID_GEOMETRY', 
               'EMPTY_GEOMETRY']].astype(int).sum(axis=1)
)

# Sort by parcels with the most issues first
issues_df = issues_df.sort_values(by='ISSUE_COUNT', ascending=False)

# --- EXPORT DATA QUALITY REPORTS ---
issues_output_path = os.path.join(output_reports, "data_quality_issues.csv")
issues_df.to_csv(issues_output_path, index=False)
logging.info(f"Exported data quality issues → {issues_output_path}")
print(f"Exported data quality issues → {issues_output_path}")

# Summary metrics
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
logging.info(f"Exported summary report → {summary_output_path}")
print(f"Exported summary report → {summary_output_path}")

# --- CREATE CLEAN AND DIRTY DATASETS ---
# Clean: parcels without any issues
clean_parcels = parcels[~(
    missing_geom_mask |
    duplicate_mask |
    date_mask |
    invalid_apn_mask |
    invalid_acreage_mask |
    invalid_geom_mask |
    empty_geom_mask
)]

# Dirty: parcels with at least one issue
dirty_parcels = parcels[
    missing_geom_mask |
    duplicate_mask |
    date_mask |
    invalid_apn_mask |
    invalid_acreage_mask |
    invalid_geom_mask |
    empty_geom_mask
]

# --- CREATE FILE GEODATABASE ---
if not arcpy.Exists(gdb_path):
    arcpy.CreateFileGDB_management(output_gdb_folder, gdb_name)
    logging.info(f"Created geodatabase → {gdb_path}")

# --- FUNCTION: Convert datetime fields to strings for shapefile export ---
def convert_datetime_to_str(df):
    """
    Convert all datetime columns in a GeoDataFrame to string format.
    Returns a copy of the dataframe with converted columns.
    """
    df_copy = df.copy()
    for col in df_copy.select_dtypes(include=['datetime64[ns]']).columns:
        df_copy[col] = df_copy[col].dt.strftime('%Y-%m-%d')
    return df_copy

clean_parcels_export = convert_datetime_to_str(clean_parcels)
dirty_parcels_export = convert_datetime_to_str(dirty_parcels)

# --- FUNCTION: Remove temporary shapefile sets ---
def remove_shapefile_set(base_path):
    """
    Deletes all files associated with a shapefile (e.g., .shp, .shx, .dbf, .prj).
    """
    base_no_ext = os.path.splitext(base_path)[0]
    for file in glob.glob(f"{base_no_ext}.*"):
        try:
            os.remove(file)
        except Exception as e:
            logging.warning(f"Could not remove {file}: {e}")

# --- EXPORT TO GDB WITH SPATIAL INDEX ---
# Original parcels
arcpy.conversion.FeatureClassToFeatureClass(parcels_path, gdb_path, "original_parcels")
arcpy.AddSpatialIndex_management(os.path.join(gdb_path, "original_parcels"))
logging.info("Exported original parcels to GDB and added spatial index")
print("Exported original parcels to GDB and added spatial index")

# --- FUNCTION: Replace string DATE_ADDED with proper date field ---
def replace_string_with_date_field(gdb_path, fc_name, string_field="DATE_ADDED", temp_date_field="DATEADDED"):
    """
    Replaces a string date field in a feature class with a true Date field.
    """
    fc_path = os.path.join(gdb_path, fc_name)
    
    # Add temporary Date field if not exists
    if temp_date_field not in [f.name for f in arcpy.ListFields(fc_path)]:
        arcpy.AddField_management(fc_path, temp_date_field, "DATE")
    
    # Calculate date values from original string field
    arcpy.CalculateField_management(
        in_table=fc_path,
        field=temp_date_field,
        expression=f"!{string_field}!",
        expression_type="PYTHON3"
    )
    
    # Delete original string field and rename temp field
    arcpy.DeleteField_management(fc_path, string_field)
    arcpy.AlterField_management(fc_path, temp_date_field, new_field_name=string_field)
    
    logging.info(f"Replaced string field {string_field} with proper Date field in {fc_name}")
    print(f"Replaced string field {string_field} with proper Date field in {fc_name}")

# --- EXPORT CLEAN AND DIRTY PARCELS TO GDB ---
# Clean parcels
temp_clean_shp = os.path.join(output_folder, "temp_clean.shp")
clean_parcels_export.to_file(temp_clean_shp)
arcpy.conversion.FeatureClassToFeatureClass(temp_clean_shp, gdb_path, "clean_parcels")
arcpy.AddSpatialIndex_management(os.path.join(gdb_path, "clean_parcels"))
logging.info("Exported clean parcels to GDB and added spatial index")
print("Exported clean parcels to GDB and added spatial index")

# Dirty parcels
temp_dirty_shp = os.path.join(output_folder, "temp_dirty.shp")
dirty_parcels_export.to_file(temp_dirty_shp)
arcpy.conversion.FeatureClassToFeatureClass(temp_dirty_shp, gdb_path, "dirty_parcels")
arcpy.AddSpatialIndex_management(os.path.join(gdb_path, "dirty_parcels"))
logging.info("Exported dirty parcels to GDB and added spatial index")
print("Exported dirty parcels to GDB and added spatial index")

# Update DATE_ADDED fields to true date fields
replace_string_with_date_field(gdb_path, "original_parcels")
replace_string_with_date_field(gdb_path, "clean_parcels")
replace_string_with_date_field(gdb_path, "dirty_parcels")
logging.info("Updated field schemas")
print("Updated field schemas")

# Remove temporary shapefiles
remove_shapefile_set(temp_clean_shp)
remove_shapefile_set(temp_dirty_shp)
logging.info("Removed temp shapefiles")
print("Removed temp shapefiles")

print("ETL complete. Original, clean, and dirty parcels are in the geodatabase.")
