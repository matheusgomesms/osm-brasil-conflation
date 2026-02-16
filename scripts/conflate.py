import geopandas as gpd
import pandas as pd
import requests
import os

BUFFER_METERS = 65

def get_osm_data(bbox):
    """
    Fetches traffic signals from Overpass API.
    """
    print("Fetching data from OpenStreetMap (Overpass API)...")
    overpass_url = "http://overpass-api.de/api/interpreter"
    
    overpass_query = f"""
    [out:json];
    node["highway"="traffic_signals"]({bbox[1]},{bbox[0]},{bbox[3]},{bbox[2]});
    out body;
    """
    
    try:
        response = requests.get(overpass_url, params={'data': overpass_query})
        data = response.json()
    except Exception as e:
        print(f"Error fetching OSM data: {e}")
        return gpd.GeoDataFrame()
    
    osm_features = []
    for element in data.get('elements', []):
        tags = element.get('tags', {})
        osm_features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [element['lon'], element['lat']]
            },
            "properties": {
                "osm_id": element['id'],
                "ref": tags.get('ref'),
                "start_date": tags.get('start_date')
            }
        })

    if not osm_features:
        return gpd.GeoDataFrame()

    return gpd.GeoDataFrame.from_features(osm_features, crs="EPSG:4326")

def run_conflation(input_file, output_dir):
    # Define output paths inside the output directory
    out_missing = os.path.join(output_dir, '1_missing_in_osm.geojson')
    out_incomplete = os.path.join(output_dir, '2_incomplete_osm_data.geojson')
    out_extra = os.path.join(output_dir, '3_extra_in_osm.geojson')

    # 1. Load Local Data
    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found.")
        return

    local_gdf = gpd.read_file(input_file)
    local_gdf = local_gdf[local_gdf.geometry.type == 'Point']

    # Rename columns for clarity
    local_gdf = local_gdf.rename(columns={
        'ref': 'local_ref', 
        'start_date': 'local_date',
        'highway': 'local_highway',
        'traffic_signals': 'local_traffic_signals'
    })
    
    # Ensure columns exist
    for col in ['local_highway', 'local_traffic_signals']:
        if col not in local_gdf.columns:
            local_gdf[col] = None

    # 2. Get OSM Data
    osm_gdf = get_osm_data(local_gdf.total_bounds)
    
    if osm_gdf.empty:
        print("No OSM traffic lights found.")
        return

    osm_gdf = osm_gdf.rename(columns={'ref': 'osm_ref', 'start_date': 'osm_date'})

    # 3. CRS Transformation (Project to meters for buffer)
    local_gdf_m = local_gdf.to_crs(epsg=3857)
    osm_gdf_m = osm_gdf.to_crs(epsg=3857)

    # 4. Buffering
    local_gdf_m = local_gdf_m.rename_geometry('geom_point')
    local_gdf_m['geom_buffer'] = local_gdf_m['geom_point'].buffer(BUFFER_METERS)
    local_gdf_m_buffered = local_gdf_m.set_geometry('geom_buffer')

    # 5. Spatial Joins
    joined_osm_to_local = gpd.sjoin(osm_gdf_m, local_gdf_m_buffered, how='left', predicate='within')
    joined_local_to_osm = gpd.sjoin(local_gdf_m_buffered, osm_gdf_m, how='left', predicate='contains')

    # --- FILE 1: MISSING IN OSM ---
    missing_in_osm = joined_local_to_osm[joined_local_to_osm.index_right.isna()].copy()
    missing_in_osm = missing_in_osm.set_geometry('geom_point')
    
    # Rename back to OSM tags
    missing_in_osm = missing_in_osm.rename(columns={
        'local_ref': 'ref', 
        'local_date': 'start_date',
        'local_highway': 'highway',
        'local_traffic_signals': 'traffic_signals'
    })
    
    missing_in_osm = missing_in_osm[['ref', 'start_date', 'highway', 'traffic_signals', 'geom_point']]
    missing_in_osm = missing_in_osm.rename_geometry('geometry').to_crs(epsg=4326)
    missing_in_osm.to_file(out_missing, driver='GeoJSON')
    print(f"Created {os.path.basename(out_missing)}: {len(missing_in_osm)} items.")

    # --- FILE 2: INCOMPLETE OSM DATA ---
    matches = joined_osm_to_local[~joined_osm_to_local.index_right.isna()].copy()
    incomplete_list = []

    for idx, row in matches.iterrows():
        osm_ref = row.get('osm_ref')
        osm_date = row.get('osm_date')
        local_ref = row.get('local_ref')
        local_date = row.get('local_date')
        
        needs_update = False
        
        # Check Ref
        if not pd.isna(local_ref) and str(local_ref).strip() != '':
            if pd.isna(osm_ref) or str(osm_ref).strip() in ['', 'nan']:
                needs_update = True
        
        # Check Date
        if not pd.isna(local_date) and str(local_date).strip() != '':
            if pd.isna(osm_date) or str(osm_date).strip() in ['', 'nan']:
                needs_update = True

        if needs_update:
            props = {
                "osm_id": row.get('osm_id'),
                "ref": str(local_ref),
                "check_date": str(local_date) if not pd.isna(local_date) else ""
            }
            incomplete_list.append({
                "type": "Feature",
                "geometry": row.geometry, 
                "properties": props
            })

    if incomplete_list:
        incomplete_gdf = gpd.GeoDataFrame.from_features(incomplete_list, crs="EPSG:3857").to_crs(epsg=4326)
        incomplete_gdf.to_file(out_incomplete, driver='GeoJSON')
        print(f"Created {os.path.basename(out_incomplete)}: {len(incomplete_gdf)} items.")
    else:
        print("No incomplete data found.")

    # --- FILE 3: EXTRA IN OSM ---
    extra_in_osm = joined_osm_to_local[joined_osm_to_local.index_right.isna()].copy()
    extra_in_osm = extra_in_osm.rename(columns={'osm_ref': 'ref', 'osm_date': 'start_date'})
    extra_in_osm = extra_in_osm[['osm_id', 'ref', 'start_date', 'geometry']]
    extra_in_osm = extra_in_osm.to_crs(epsg=4326)
    extra_in_osm.to_file(out_extra, driver='GeoJSON')
    print(f"Created {os.path.basename(out_extra)}: {len(extra_in_osm)} items.")