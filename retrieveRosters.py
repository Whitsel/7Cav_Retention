
# Description: Python script to retrieve rosters from 7Cav API and save them to JSON files.
# This script fetches rosters for different types (e.g., COMBAT, RESERVE) and saves them to separate JSON files.

import json
import os
import requests

def fetch_roster(api_url, roster_type, headers):
    """Fetch roster data from API for a given roster type."""
    response = requests.get(f"{api_url}/api/v1/roster/ROSTER_TYPE_{roster_type}/lite", headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch roster {roster_type}. Status Code: {response.status_code}")
        return None

def save_roster(roster_type, data, save_dir):
    """Save roster data to a JSON file."""
    os.makedirs(save_dir, exist_ok=True)
    file_path = os.path.join(save_dir, f"roster_{roster_type}.json")
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)
    print(f"[✔] Saved roster: {file_path}")

def main():
    api_url = "https://api.7cav.us"  # Base API URL
    save_dir = "Rosters"  # Directory to save rosters
    
    # Manually set API key
    API_KEY = "<<YOUR API KEY HERE>>"  # Replace this with your actual API key
    headers = {"Authorization": f"Bearer {API_KEY}"}  # Adjust header format if needed
    
    # List of roster types to fetch
    roster_types = ["COMBAT", "RESERVE", "ELOA", "PAST_MEMBERS", "WALL_OF_HONOR", "ARLINGTON", "UNSPECIFIED"]  # Adjust as needed
    
    for roster_type in roster_types:
        print(f"Fetching roster: {roster_type}")
        roster_data = fetch_roster(api_url, roster_type, headers)
        if roster_data:
            num_members = len(roster_data.get("profiles", {}))
            print(f"  -> Members found in {roster_type}: {num_members}")
            save_roster(roster_type, roster_data, save_dir)
    
    print("[✔] All rosters retrieved and saved successfully.")

if __name__ == "__main__":
    main()
