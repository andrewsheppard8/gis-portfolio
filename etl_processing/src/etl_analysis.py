import geopandas as gpd
import matplotlib.pyplot as plt
import folium
import os

BASE_DIR = os.path.dirname(os.path.dirname(__file__))

def Add_Parcels():
    try:
        parcels_path = os.path.join(BASE_DIR, "data", "raw", "PARCELS_CREST.shp")
        schools_path = os.path.join(BASE_DIR, "data", "raw", "School_Districts.shp")
        csv_path= os.path.join(BASE_DIR, "data", "output", "single_family_counts_by_district.csv")
        parcels = gpd.read_file(parcels_path)
        schools = gpd.read_file(schools_path)
        print(f"[INFO] Parcels loaded: {parcels.shape}")
        print(f"[INFO] Schools loaded: {schools.shape}")
        print("[INFO] Parcel columns:", parcels.columns.tolist())
        print("[INFO] School columns:", schools.columns.tolist())
        total_parcels=parcels.shape[0]
        single_family_parcels = parcels[
            parcels['CLASS_CODE'].str.strip().str.lower() == 'single family dwelling'
            ]
        single_family_count=single_family_parcels.shape[0]
        proportion=single_family_count/total_parcels
        print(f"Single Family Dwelling parcels: {single_family_count}")
        print(f"Total parcels: {total_parcels}")
        print(f"Proportion: {proportion:.2%}")
        # Step 1: Ensure same CRS
        single_family_parcels = single_family_parcels.to_crs(schools.crs)
        # Step 2: Spatial join parcels with school districts
        parcels_with_district = gpd.sjoin(single_family_parcels, schools[['DISTRICT_N', 'geometry']], 
                                        how='left', predicate='intersects')
        # Step 3: Count single-family parcels per district
        parcel_counts_by_district = parcels_with_district.groupby('DISTRICT_N').size().reset_index(name='single_fam')
        # Step 4: Sort descending
        parcel_counts_by_district = parcel_counts_by_district.sort_values(by='single_fam', ascending=False)
        # Step 5: Print results
        print("Total parcels after spatial join:", parcels_with_district.shape[0])
        print("Sample of parcels with district info:")
        print(parcels_with_district[['CLASS_CODE', 'DISTRICT_N']].head())
        parcel_counts_by_district.to_csv(csv_path, index=False)
        print("CSV created")
        # Merge the counts back into the school districts GeoDataFrame
        schools_with_counts = schools.merge(parcel_counts_by_district, on='DISTRICT_N', how='left')
        # Fill any districts with no single-family parcels with 0
        schools_with_counts['single_fam'] = schools_with_counts['single_fam'].fillna(0)
        # Save to a new shapefile
        schools_with_counts.to_file(os.path.join(BASE_DIR, "data", "output", "School_Districts_with_SF_Counts.shp"))
        print("New School District Shapefile loaded:", schools_with_counts.shape)
        print("New Columns in Shapefile:", schools_with_counts.columns.tolist())
        return schools_with_counts
    except Exception as e:
        print("[ERROR] Error loading files:", e)

def Map_Districts(schools_with_counts):
    # Plot a choropleth
    fig, ax = plt.subplots(1, 1, figsize=(12, 8))

    # Plot districts, coloring by single_family_count
    schools_with_counts.plot(column='single_fam', 
                            cmap='OrRd',         # Color map
                            legend=True,         # Show color legend
                            edgecolor='black',   # District boundaries
                            linewidth=0.5, 
                            ax=ax)

    # Add title
    ax.set_title('Single Family Parcels per School District', fontsize=16)

    # Remove axis
    ax.set_axis_off()

    # Show the plot
    plt.show()
    try:
        fig.savefig(os.path.join(BASE_DIR, "data", "output", "SF_Parcels_by_District.png"), dpi=300)
        print("Map saved as image")
    except Exception as e:
        print("[ERROR] Error printing image",e)

def Map_Districts_Web(schools_with_counts):
    # print(schools_with_counts.columns)

    # Step 1: Project to a projected CRS (Web Mercator is fine)
    projected = schools_with_counts.to_crs(epsg=3857)

    # Step 2: Get centroid in projected space
    centroid = projected.geometry.centroid

    # Step 3: Convert centroid back to WGS84
    centroid = gpd.GeoSeries(centroid, crs=3857).to_crs(epsg=4326)

    # 🔥 IMPORTANT: convert ORIGINAL polygons to WGS84 for mapping
    schools_with_counts = schools_with_counts.to_crs(epsg=4326)

    # Step 4: Use for map center
    center = [centroid.y.mean(), centroid.x.mean()]

    # schools_with_counts = schools_with_counts.to_crs(epsg=4326)

    # Center map
    m = folium.Map(location=center, zoom_start=10, tiles='CartoDB positron')

    # Convert GeoDataFrame to GeoJSON using __geo_interface__
    geojson_data = schools_with_counts.__geo_interface__

    folium.Choropleth(
        geo_data=geojson_data,
        name='choropleth',
        data=schools_with_counts,
        columns=['DISTRICT_N', 'single_fam'],
        key_on='feature.properties.DISTRICT_N',
        fill_color='YlOrRd',
        fill_opacity=0.7,
        line_opacity=0.5,
        legend_name='Single Family Parcels'
    ).add_to(m)

    folium.GeoJson(
    schools_with_counts,
    tooltip=folium.GeoJsonTooltip(
        fields=['DISTRICT_N', 'single_fam'],
        aliases=['District:', 'Single Family Parcels:']
    )
    ).add_to(m)

    try:
        # Save
        m.save(os.path.join(BASE_DIR, "data", "output","SF_Parcels_by_District_Map.html"))
        print("Web map saved!")
    except Exception as e:
        print("[ERROR] Error creating webpage",e)

def main():
    schools_with_counts = Add_Parcels()
    if schools_with_counts is not None:
        Map_Districts(schools_with_counts)
        Map_Districts_Web(schools_with_counts)
    else:
        print("[ERROR] Parcel processing failed.")

if __name__ == "__main__":
    main()