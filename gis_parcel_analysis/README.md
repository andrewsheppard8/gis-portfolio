# Single Family Parcels by School District



## Project Title

Single Family Parcels by School District – GIS Analysis \& Visualization



### Description

This project analyzes and visualizes single-family dwelling parcels within school districts. It uses Python, GeoPandas, and Folium to:



Load and clean parcel and school district shapefiles

Filter parcels for “Single Family Dwelling” class

Perform spatial joins with school districts

Aggregate single-family parcel counts per district

Export results to CSV and a new shapefile

Visualize results with static maps (Matplotlib) and interactive web maps (Folium)



The workflow demonstrates end-to-end GIS processing including spatial joins, CRS management, aggregation, and both static and web visualization.



### Technologies \& Libraries

###### Python 3.11+

GeoPandas

&#x09;geospatial data processing

Matplotlib

&#x20;	static mapping

Folium

&#x09;interactive web mapping

Shapely

&#x09;geometry operations

OS

&#x09;file path management



### Features

###### Data Processing

&#x09;Filters parcels to only single-family dwellings

&#x09;Ensures coordinate reference system (CRS) alignment

&#x09;Aggregates parcel counts per school district

###### Outputs

&#x09;CSV of single-family parcel counts per district

&#x09;Shapefile with counts merged into school districts

&#x09;Static choropleth map (PNG)

&#x09;Interactive web map (HTML)

###### CRS Handling

&#x09;Projected for centroid calculations

&#x09;Converted back to WGS84 for web mapping



### File Structure



project-root/

│

├─ data/

│   ├─ raw/                      # Original shapefiles

│   │   ├─ PARCELS\_CREST.shp

│   │   └─ School\_Districts.shp

│   └─ output/                   # Generated files

│       ├─ single\_family\_counts\_by\_district.csv

│       ├─ School\_Districts\_with\_SF\_Counts.shp

│       ├─ SF\_Parcels\_by\_District.png

│       └─ SF\_Parcels\_by\_District\_Map.html

│

├─ scripts/

│   └─ gis\_parcel\_analysis.py    # Main Python script

│

└─ README.md



### Notes/Future Improvements

Could add multiple parcel classes as filter options

Could integrate Flask or Streamlit for an interactive app interface

Add unit tests for spatial join and aggregation functions

Include automated download or validation of raw shapefiles



### Author

Andrew Sheppard – GIS Developer / Python Enthusiast

GitHub: https://github.com/andrewsheppard8

Email: andrewsheppard8@gmail.com

