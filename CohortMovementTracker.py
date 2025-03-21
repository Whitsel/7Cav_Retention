import json
import matplotlib.pyplot as plt
import os
import sys
import re
from datetime import datetime

def load_json(file_path):
    """Load the JSON data from a file."""
    print(f"Looking for file at: {os.path.abspath(file_path)}")
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def convert_letter_to_number(letter):
    """Convert leading letter to corresponding number where applicable."""
    letter_map = {
        'A': '1', 'B': '2', 'C': '3', 'D': '4', 'E': '5',
        'F': '6', 'G': '7', 'H': '8', 'I': '9'
    }
    return letter_map.get(letter, letter)

def normalize_cohort_format(cohort):
    """Normalize cohort group format to use numbers and letters in the format X/X/X/X-X."""
    cohort = cohort.replace(",", "")  # Remove commas
    
    # Handle Boot Camp records
    if re.match(r"\d{3}/\d{2}/\d{2}", cohort):
        return "Boot Camp"
    
    parts = cohort.split("/")
    
    # Convert leading letter if applicable
    if parts and parts[0] in "ABCDEFGHI":
        parts[0] = convert_letter_to_number(parts[0])
    
    return "/".join(parts) if len(parts) > 1 else "Unknown"

def extract_cohort_movements(user_data):
    """Extract cohort group changes from transfer and discharge records."""
    movements = []
    for record in user_data.get("records", []):
        date = datetime.strptime(record["recordDate"], "%Y-%m-%d")
        
        if record["recordType"] == "RECORD_TYPE_TRANSFER":
            details = record["recordDetails"]
            words = details.split()
            cohort_group = next((word for word in words if "/" in word), "Unknown")
            cohort_group = normalize_cohort_format(cohort_group)
        
        elif record["recordType"] == "RECORD_TYPE_DISCHARGE":
            details = record["recordDetails"].lower()
            if "retired" in details:
                cohort_group = "Retired"
            else:
                cohort_group = "Discharged"
        else:
            continue
        
        movements.append({
            "date": date,
            "cohort": cohort_group
        })
    
    return sorted(movements, key=lambda x: x["date"])

def plot_cohort_movements(movements, username):
    """Plot cohort movements over time."""
    dates = [m["date"] for m in movements]
    cohorts = [m["cohort"] for m in movements]
    
    plt.figure(figsize=(10, 5))
    plt.plot(dates, cohorts, marker="o", linestyle="-", color="b")
    
    plt.xlabel("Date")
    plt.ylabel("Cohort Group")
    plt.title(f"Cohort Movement Over Time for {username}")
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.show()

# Ensure correct file path
if len(sys.argv) > 1:
    file_path = sys.argv[1]
else:
    file_path = r"C:\Users\micah\Downloads\milpacs_profile_76.json"  # Change if needed

# Load user data
user_data = load_json(file_path)

# Extract user details
username = user_data["user"]["username"]

# Extract and visualize cohort movements
movements = extract_cohort_movements(user_data)
plot_cohort_movements(movements, username)
