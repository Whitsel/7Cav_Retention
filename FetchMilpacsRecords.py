
# Description: Fetch milpacs records for all users in multiple roster JSON files using the 7Cav API.
# This script reads all roster JSON files in the Rosters directory, extracts user IDs, and fetches milpacs records for each user.
# The milpacs records are saved as separate JSON files in the Milpacs directory.
# The script uses multi-threading to speed up API calls while respecting rate limits.
# Note: This script requires a valid API key with the necessary permissions to fetch milpacs data.
# Make sure to replace the API key in the script with your actual key before running it.
# The API base URL and other parameters can be adjusted as needed.
# Usage: python FetchMilpacsRecords.py

import json
import os
import requests
import concurrent.futures
from time import sleep

def load_roster(file_path):
    """Load the roster JSON file."""
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def fetch_milpacs_profile(user_id, api_url, headers):
    """Fetch milpacs profile data from API for a given user ID with authentication."""
    response = requests.get(f"{api_url}/api/v1/milpacs/profile/id/{user_id}", headers=headers)
    if response.status_code == 200:
        return response.json()
    elif response.status_code == 404:
        print(f"[⚠] No milpacs record found for user {user_id} (404 Not Found)")
        log_failed_request(user_id)
        return None
    else:
        print(f"[⚠] Failed to fetch data for user {user_id}. Status Code: {response.status_code}")
        log_failed_request(user_id)
        return None

def log_failed_request(user_id):
    """Log failed requests to a file for later review."""
    with open("failed_requests.log", "a") as log_file:
        log_file.write(f"User ID {user_id} - Failed to fetch milpacs record\n")

def save_milpacs_record(user_id, data, save_dir):
    """Save milpacs profile data to a JSON file."""
    os.makedirs(save_dir, exist_ok=True)
    file_path = os.path.join(save_dir, f"{user_id}.json")
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)
    print(f"[✔] Saved milpacs record for user {user_id}.")

def process_roster_files(roster_dir, api_url, save_dir, headers):
    """Process all roster JSON files in the Rosters directory using multi-threading."""
    total_files = len([f for f in os.listdir(roster_dir) if f.endswith(".json")])
    file_count = 0
    
    user_ids = []
    for file_name in os.listdir(roster_dir):
        if file_name.endswith(".json"):
            file_path = os.path.join(roster_dir, file_name)
            file_count += 1
            print(f"Processing roster file {file_count}/{total_files}: {file_path}")
            roster_data = load_roster(file_path)
            
            profiles = roster_data.get("profiles", {})
            total_members = len(profiles)
            print(f"  -> Total members found in {file_name}: {total_members}")
            
            if total_members == 0:
                print(f"  [⚠] WARNING: No members found in {file_name}. Check JSON structure.")
                continue
            
            for profile_id, profile in profiles.items():
                user_id = profile_id
                if user_id:
                    user_ids.append(user_id)
                else:
                    print(f"      [⚠] Skipping member, missing 'userId' field: {profile}")
    
    print(f"[✔] Fetching milpacs records for {len(user_ids)} users using multi-threading.")
    
    # Use threading to speed up API calls while respecting rate limits
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_milpacs_profile, user_id, api_url, headers): user_id for user_id in user_ids}
        for future in concurrent.futures.as_completed(futures):
            user_id = futures[future]
            try:
                milpacs_data = future.result()
                if milpacs_data:
                    save_milpacs_record(user_id, milpacs_data, save_dir)
            except Exception as e:
                print(f"[⚠] Error fetching milpacs for user {user_id}: {e}")
                log_failed_request(user_id)

def main():
    roster_dir = "Rosters"  # Directory containing multiple roster files
    api_url = "https://api.7cav.us"  # Updated API base URL with correct endpoint
    save_dir = "Milpacs"
    
    # Manually set API key
    API_KEY = "<<YOUR API KEY HERE>>"  # Replace this with your actual API key
    headers = {"Authorization": f"Bearer {API_KEY}"}  # Adjust header format if needed
    
    # Clear old failed requests log
    open("failed_requests.log", "w").close()
    
    process_roster_files(roster_dir, api_url, save_dir, headers)
    print("[✔] All milpacs records retrieved and saved successfully.")
    print("[⚠] Check 'failed_requests.log' for missing records.")

if __name__ == "__main__":
    main()
