import geopandas as gpd
import pandas as pd
import os

# --- PATHS ---
parcels_path = r"C:\Users\andre\Desktop\GIT Resources\gis-portfolio\etl_processing\data\raw\PARCELS_CREST.shp"
output_folder = r"C:\Users\andre\Desktop\GIT Resources\gis-portfolio\etl_processing\data\output"

# Ensure output folder exists
os.makedirs(output_folder, exist_ok=True)

# --- EXTRACT ---
parcels = gpd.read_file(parcels_path)

# --- TRANSFORM: DATA QUALITY FLAGS ---

# Total count
total_parcels = len(parcels)

# Missing geometry flag
missing_geom_mask = parcels['geometry'].isna()

# Duplicate APN flag (ALL duplicates)
duplicate_mask = parcels.duplicated(subset=['APN'], keep=False) #if keep='first' will only list the duplicates, not the first instance

# Create issues DataFrame
issues_df = parcels[['APN']].copy()
issues_df['MISSING_GEOMETRY'] = missing_geom_mask
issues_df['DUPLICATE_APN'] = duplicate_mask

# Keep only rows with at least one issue
issues_df = issues_df[
    (issues_df['MISSING_GEOMETRY']) | (issues_df['DUPLICATE_APN'])
]

# Add issue count
issues_df['ISSUE_COUNT'] = (
    issues_df['MISSING_GEOMETRY'].astype(int) +
    issues_df['DUPLICATE_APN'].astype(int)
)

# Sort by worst records first
issues_df = issues_df.sort_values(by='ISSUE_COUNT', ascending=False)

# --- LOAD: EXPORT DATA QUALITY TABLE ---
issues_output_path = os.path.join(output_folder, "data_quality_issues.csv")
issues_df.to_csv(issues_output_path, index=False)

print(f"Exported data quality issues → {issues_output_path}")

# --- SUMMARY REPORT ---
missing_geom_count = missing_geom_mask.sum()
duplicate_count = duplicate_mask.sum()
unique_duplicate_apns = parcels.loc[duplicate_mask, 'APN'].nunique()

summary_df = pd.DataFrame({
    "Metric": [
        "Total Parcels",
        "Missing Geometry",
        "Duplicate Records",
        "Unique Duplicate APNs"
    ],
    "Value": [
        total_parcels,
        missing_geom_count,
        duplicate_count,
        unique_duplicate_apns
    ]
})

summary_output_path = os.path.join(output_folder, "etl_summary_report.csv")
summary_df.to_csv(summary_output_path, index=False)

print(f"Exported summary report → {summary_output_path}")
