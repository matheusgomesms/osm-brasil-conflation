import json
from datetime import datetime

IGNORE_STATUS = ['DESATIVADO', 'PROJETO']

def format_date(date_str):
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return date_str 

def process_clean(input_path, output_path):
    try:
        with open(input_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: File {input_path} not found.")
        return

    signals_features = []
    
    skipped_count = 0

    for feature in data['features']:
        props = feature['properties']
        coords = feature['geometry']['coordinates']
        
        status = props.get('STATUS', '').upper()
        
        if status in IGNORE_STATUS:
            skipped_count += 1
            continue 
        
        ts_type = "signal"
        if props.get('SEMÁFORO_EXCLUSIVO_PEDESTRE') == 'S':
            ts_type = "pedestrian_crossing"

        new_props_signal = {
            "highway": "traffic_signals",
            "traffic_signals": ts_type,
            "ref": props.get('CÓDIGO'),
            "start_date": format_date(props.get('DATA_IMPLANTAÇÃO'))
        }

        # Remove keys with None values
        new_props_signal = {k: v for k, v in new_props_signal.items() if v is not None}

        signal_feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": coords
            },
            "properties": new_props_signal
        }
        signals_features.append(signal_feature)

    # Save Result to the specific output path provided by main.py
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump({"type": "FeatureCollection", "features": signals_features}, f, indent=2, ensure_ascii=False)
    
    print(f"Skipped {skipped_count} ignored items.")
    print(f"Saved cleaned data to {output_path} ({len(signals_features)} features).")