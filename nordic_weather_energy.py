#!/usr/bin/env python3
"""
Nordic Weather Forecast Fetcher for Energy Price Forecasting
Fetches weather forecasts from SMHI API for multiple locations across Nordic countries
with focus on energy-relevant locations
"""

import requests
import json
from datetime import datetime, timezone
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# Define locations relevant for energy price forecasting
NORDIC_LOCATIONS = {
    "Sweden": [
        # Major cities and population centers (high electricity demand)
        {"name": "Stockholm", "lat": 59.3293, "lon": 18.0686, "type": "major_city"},
        {"name": "Gothenburg", "lat": 57.7089, "lon": 11.9746, "type": "major_city"},
        {"name": "Malmö", "lat": 55.6050, "lon": 13.0038, "type": "major_city"},
        {"name": "Uppsala", "lat": 59.8586, "lon": 17.6389, "type": "city"},
        {"name": "Västerås", "lat": 59.6099, "lon": 16.5448, "type": "city"},
        {"name": "Örebro", "lat": 59.2753, "lon": 15.2134, "type": "city"},
        {"name": "Linköping", "lat": 58.4108, "lon": 15.6214, "type": "city"},
        {"name": "Helsingborg", "lat": 56.0465, "lon": 12.6945, "type": "city"},
        {"name": "Jönköping", "lat": 57.7826, "lon": 14.1618, "type": "city"},
        {"name": "Norrköping", "lat": 58.5877, "lon": 16.1924, "type": "city"},
        
        # Northern Sweden - Hydropower regions
        {"name": "Luleå", "lat": 65.5848, "lon": 22.1547, "type": "hydro_region"},
        {"name": "Kiruna", "lat": 67.8558, "lon": 20.2253, "type": "hydro_region"},
        {"name": "Umeå", "lat": 63.8258, "lon": 20.2630, "type": "hydro_region"},
        {"name": "Sundsvall", "lat": 62.3908, "lon": 17.3069, "type": "hydro_region"},
        {"name": "Östersund", "lat": 63.1792, "lon": 14.6357, "type": "hydro_region"},
        {"name": "Gällivare", "lat": 67.1355, "lon": 20.6670, "type": "hydro_region"},
        
        # Wind power areas (coastal and southern Sweden)
        {"name": "Gotland (Visby)", "lat": 57.6348, "lon": 18.2948, "type": "wind_region"},
        {"name": "Öland (Borgholm)", "lat": 56.8797, "lon": 16.6564, "type": "wind_region"},
        {"name": "Halland (Varberg)", "lat": 57.1057, "lon": 12.2508, "type": "wind_region"},
        {"name": "Skåne (Kristianstad)", "lat": 56.0294, "lon": 14.1567, "type": "wind_region"},
        {"name": "Blekinge (Karlskrona)", "lat": 56.1612, "lon": 15.5869, "type": "wind_region"},
        
        # Industrial centers (high energy consumption)
        {"name": "Borlänge", "lat": 60.4858, "lon": 15.4371, "type": "industrial"},
        {"name": "Sandviken", "lat": 60.6160, "lon": 16.7709, "type": "industrial"},
        {"name": "Trollhättan", "lat": 58.2837, "lon": 12.2886, "type": "industrial"},
    ],
    
    "Norway": [
        # Major cities
        {"name": "Oslo", "lat": 59.9139, "lon": 10.7522, "type": "major_city"},
        {"name": "Bergen", "lat": 60.3913, "lon": 5.3221, "type": "major_city"},
        {"name": "Trondheim", "lat": 63.4305, "lon": 10.3951, "type": "major_city"},
        {"name": "Stavanger", "lat": 58.9700, "lon": 5.7331, "type": "major_city"},
        
        # Hydropower regions
        {"name": "Tromsø", "lat": 69.6492, "lon": 18.9553, "type": "hydro_region"},
        {"name": "Bodø", "lat": 67.2804, "lon": 14.4049, "type": "hydro_region"},
        {"name": "Ålesund", "lat": 62.4722, "lon": 6.1549, "type": "hydro_region"},
        {"name": "Kristiansand", "lat": 58.1599, "lon": 8.0182, "type": "hydro_region"},
    ],
    
    "Finland": [
        # Major cities
        {"name": "Helsinki", "lat": 60.1699, "lon": 24.9384, "type": "major_city"},
        {"name": "Espoo", "lat": 60.2055, "lon": 24.6559, "type": "major_city"},
        {"name": "Tampere", "lat": 61.4978, "lon": 23.7610, "type": "major_city"},
        {"name": "Turku", "lat": 60.4518, "lon": 22.2666, "type": "major_city"},
        {"name": "Oulu", "lat": 65.0121, "lon": 25.4651, "type": "major_city"},
        
        # Nuclear power locations
        {"name": "Loviisa", "lat": 60.3681, "lon": 26.3581, "type": "nuclear"},
        {"name": "Eurajoki (Olkiluoto)", "lat": 61.2351, "lon": 21.4845, "type": "nuclear"},
        
        # Wind power regions
        {"name": "Vaasa", "lat": 63.0960, "lon": 21.6158, "type": "wind_region"},
        {"name": "Lapland (Rovaniemi)", "lat": 66.5039, "lon": 25.7294, "type": "wind_region"},
    ],
    
    "Denmark": [
        # Major cities
        {"name": "Copenhagen", "lat": 55.6761, "lon": 12.5683, "type": "major_city"},
        {"name": "Aarhus", "lat": 56.1629, "lon": 10.2039, "type": "major_city"},
        {"name": "Odense", "lat": 55.4038, "lon": 10.4024, "type": "major_city"},
        {"name": "Aalborg", "lat": 57.0488, "lon": 9.9187, "type": "major_city"},
        
        # Wind power regions
        {"name": "Esbjerg", "lat": 55.4669, "lon": 8.4597, "type": "wind_region"},
        {"name": "Ringkøbing", "lat": 56.0905, "lon": 8.2440, "type": "wind_region"},
        {"name": "Bornholm (Rønne)", "lat": 55.1003, "lon": 14.7005, "type": "wind_region"},
    ]
}

def get_smhi_forecast(location):
    """
    Fetch weather forecast from SMHI API for given location
    
    Args:
        location: dict with 'name', 'lat', 'lon', 'type' keys
    
    Returns:
        tuple: (location, forecast_data or None)
    """
    url = f"https://opendata-download-metfcst.smhi.se/api/category/pmp3g/version/2/geotype/point/lon/{location['lon']}/lat/{location['lat']}/data.json"
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return location, response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {location['name']}: {e}")
        return location, None

def extract_todays_forecast(data):
    """
    Extract today's forecast from SMHI data
    
    Args:
        data: SMHI API response
    
    Returns:
        list: Today's forecast entries
    """
    if not data or 'timeSeries' not in data:
        return []
    
    today = datetime.now(timezone.utc).date()
    todays_forecast = []
    
    for entry in data['timeSeries']:
        valid_time = datetime.fromisoformat(entry['validTime'].replace('Z', '+00:00'))
        if valid_time.date() == today:
            todays_forecast.append(entry)
    
    return todays_forecast

def extract_energy_relevant_data(forecast_entries):
    """
    Extract weather parameters most relevant for energy price forecasting
    
    Args:
        forecast_entries: List of forecast entries
    
    Returns:
        dict: Energy-relevant weather data
    """
    if not forecast_entries:
        return None
    
    energy_data = {
        'hourly_data': [],
        'daily_summary': {
            'temp_min': float('inf'),
            'temp_max': float('-inf'),
            'temp_avg': 0,
            'wind_speed_avg': 0,
            'wind_speed_max': 0,
            'precipitation_total': 0,
            'cloud_cover_avg': 0,
        }
    }
    
    temp_sum = 0
    wind_sum = 0
    cloud_sum = 0
    count = 0
    
    for entry in forecast_entries:
        time = datetime.fromisoformat(entry['validTime'].replace('Z', '+00:00'))
        params = {p['name']: p['values'][0] for p in entry['parameters']}
        
        hourly = {
            'time': time.isoformat(),
            'temperature': params.get('t', None),
            'wind_speed': params.get('ws', None),
            'wind_direction': params.get('wd', None),
            'precipitation': params.get('pmean', None),
            'humidity': params.get('r', None),
            'pressure': params.get('msl', None),
            'cloud_cover': params.get('tcc_mean', None),  # Total cloud cover
            'wind_gust': params.get('gust', None),
        }
        
        energy_data['hourly_data'].append(hourly)
        
        # Update summary statistics
        if hourly['temperature'] is not None:
            temp = hourly['temperature']
            energy_data['daily_summary']['temp_min'] = min(energy_data['daily_summary']['temp_min'], temp)
            energy_data['daily_summary']['temp_max'] = max(energy_data['daily_summary']['temp_max'], temp)
            temp_sum += temp
            
        if hourly['wind_speed'] is not None:
            wind = hourly['wind_speed']
            wind_sum += wind
            energy_data['daily_summary']['wind_speed_max'] = max(energy_data['daily_summary']['wind_speed_max'], wind)
            
        if hourly['precipitation'] is not None:
            energy_data['daily_summary']['precipitation_total'] += hourly['precipitation']
            
        if hourly['cloud_cover'] is not None:
            cloud_sum += hourly['cloud_cover']
            
        count += 1
    
    # Calculate averages
    if count > 0:
        energy_data['daily_summary']['temp_avg'] = round(temp_sum / count, 1)
        energy_data['daily_summary']['wind_speed_avg'] = round(wind_sum / count, 1)
        energy_data['daily_summary']['cloud_cover_avg'] = round(cloud_sum / count, 1)
    
    return energy_data

def save_all_forecasts(all_forecasts, output_dir="weather_forecasts"):
    """
    Save all forecasts to files organized by country and type
    
    Args:
        all_forecasts: Dict with country -> list of (location, forecast_data)
        output_dir: Base directory for output
    """
    # For GitHub Actions, use date only (no time) to allow file updates
    timestamp = datetime.now().strftime('%Y-%m-%d')
    base_dir = f"{output_dir}/{timestamp}"
    
    # Create directory structure
    os.makedirs(base_dir, exist_ok=True)
    
    # Prepare combined data for analysis
    combined_data = {
        'metadata': {
            'timestamp': datetime.now().isoformat(),
            'total_locations': sum(len(locations) for locations in all_forecasts.values()),
            'countries': list(all_forecasts.keys())
        },
        'forecasts': {}
    }
    
    # Save by country and type
    for country, forecasts in all_forecasts.items():
        country_dir = f"{base_dir}/{country}"
        os.makedirs(country_dir, exist_ok=True)
        
        country_data = {
            'locations': [],
            'by_type': {}
        }
        
        for location, data in forecasts:
            if data:
                todays_forecast = extract_todays_forecast(data)
                energy_data = extract_energy_relevant_data(todays_forecast)
                
                location_data = {
                    'location': location,
                    'energy_relevant_data': energy_data,
                    'raw_forecast': todays_forecast
                }
                
                country_data['locations'].append(location_data)
                
                # Group by type
                loc_type = location['type']
                if loc_type not in country_data['by_type']:
                    country_data['by_type'][loc_type] = []
                country_data['by_type'][loc_type].append(location_data)
        
        # Save country file
        country_file = f"{country_dir}/all_forecasts.json"
        with open(country_file, 'w', encoding='utf-8') as f:
            json.dump(country_data, f, indent=2, ensure_ascii=False)
        
        combined_data['forecasts'][country] = country_data
    
    # Save combined file for easy analysis
    combined_file = f"{base_dir}/nordic_energy_weather_data.json"
    with open(combined_file, 'w', encoding='utf-8') as f:
        json.dump(combined_data, f, indent=2, ensure_ascii=False)
    
    # Create summary CSV for quick overview
    summary_file = f"{base_dir}/weather_summary.csv"
    with open(summary_file, 'w', encoding='utf-8') as f:
        f.write("Country,Location,Type,Lat,Lon,Temp_Min,Temp_Max,Temp_Avg,Wind_Avg,Wind_Max,Precip_Total,Cloud_Avg\n")
        
        for country, country_data in combined_data['forecasts'].items():
            for loc_data in country_data['locations']:
                loc = loc_data['location']
                energy = loc_data['energy_relevant_data']
                if energy:
                    summary = energy['daily_summary']
                    f.write(f"{country},{loc['name']},{loc['type']},{loc['lat']},{loc['lon']},")
                    f.write(f"{summary['temp_min']},{summary['temp_max']},{summary['temp_avg']},")
                    f.write(f"{summary['wind_speed_avg']},{summary['wind_speed_max']},")
                    f.write(f"{summary['precipitation_total']},{summary['cloud_cover_avg']}\n")
    
    print(f"\nAll forecasts saved to: {base_dir}")
    print(f"  - Combined data: {combined_file}")
    print(f"  - Summary CSV: {summary_file}")
    print(f"  - Country-specific files in: {base_dir}/[country]/all_forecasts.json")

def main():
    print("Nordic Weather Forecast Fetcher for Energy Price Forecasting")
    print("=" * 60)
    
    all_forecasts = {}
    total_locations = sum(len(locations) for locations in NORDIC_LOCATIONS.values())
    
    print(f"\nFetching forecasts for {total_locations} locations across Nordic countries...")
    print(f"This includes major cities, hydropower regions, wind power areas, and industrial centers.\n")
    
    # Process each country
    for country, locations in NORDIC_LOCATIONS.items():
        print(f"\nProcessing {country} ({len(locations)} locations)...")
        country_forecasts = []
        
        # Use ThreadPoolExecutor for parallel fetching
        with ThreadPoolExecutor(max_workers=10) as executor:
            # Submit all requests
            future_to_location = {
                executor.submit(get_smhi_forecast, loc): loc 
                for loc in locations
            }
            
            # Process completed requests
            for future in as_completed(future_to_location):
                location, data = future.result()
                country_forecasts.append((location, data))
                
                if data:
                    print(f"  ✓ {location['name']} ({location['type']})")
                else:
                    print(f"  ✗ {location['name']} ({location['type']}) - Failed")
                
                # Small delay to avoid overwhelming the API
                time.sleep(0.1)
        
        all_forecasts[country] = country_forecasts
    
    # Save all data
    print("\nSaving forecast data...")
    save_all_forecasts(all_forecasts)
    
    # Print summary statistics
    successful = sum(1 for forecasts in all_forecasts.values() 
                    for _, data in forecasts if data is not None)
    print(f"\nFetch Summary:")
    print(f"  - Total locations: {total_locations}")
    print(f"  - Successful: {successful}")
    print(f"  - Failed: {total_locations - successful}")
    
    print("\nData includes energy-relevant parameters:")
    print("  - Temperature (current, min, max, average)")
    print("  - Wind speed and direction (crucial for wind power)")
    print("  - Precipitation (affects hydropower)")
    print("  - Cloud cover (affects solar in summer)")
    print("  - Atmospheric pressure")
    print("\nLocations chosen based on:")
    print("  - Major population centers (high demand)")
    print("  - Hydropower regions (Northern Sweden/Norway)")
    print("  - Wind power areas (coastal and southern regions)")
    print("  - Industrial centers (energy-intensive industries)")
    print("  - Nuclear power plants (Finland)")

if __name__ == "__main__":
    main()