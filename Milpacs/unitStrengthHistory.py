import json
import os
import re
import pandas as pd
from datetime import datetime, timedelta

# Load and parse JSON file
def load_json(filepath):
    with open(filepath) as f:
        return json.load(f)

# Normalize squad identifiers (letters to numbers)
def normalize_squad(squad):
    if squad is None:
        return None
    if squad.isalpha():
        return str(ord(squad.upper()) - ord('A') + 1)
    return squad

# Parse unit strings into structured data based on acceptable formats
def parse_unit(title):
    battalion_pattern = re.compile(r'(1-7|2-7|3-7|ACD)')
    company_pattern = re.compile(r'\b([A-Z])\b')
    platoon_pattern = re.compile(r'\b(\d)\b')
    squad_pattern = re.compile(r'\b([A-Z]|\d)\b')

    parts = title.split('/')

    # Helper function to safely extract matches
    def safe_extract(pattern, text):
        match = pattern.findall(text)
        return match[0] if match else None

    squad = normalize_squad(safe_extract(squad_pattern, parts[0]) if len(parts) > 0 else None)
    platoon = safe_extract(platoon_pattern, parts[1]) if len(parts) > 1 else None
    company = safe_extract(company_pattern, parts[2]) if len(parts) > 2 else None
    battalion = safe_extract(battalion_pattern, parts[3]) if len(parts) > 3 else None

    return {
        'Squad': squad,
        'Platoon': platoon,
        'Company': company,
        'Battalion': battalion,
    }

# Extract transfer and discharge records
def extract_events(data):
    events = []

    for record in data.get("records", []):
        record_type = record["recordType"]
        record_date = datetime.strptime(record["recordDate"], "%Y-%m-%d")
        details = record["recordDetails"]

        if record_type == "RECORD_TYPE_TRANSFER":
            unit_str = details.split("Assigned")[-1].strip()
            unit = parse_unit(unit_str)
            events.append({
                'date': record_date,
                'event': 'transfer',
                **unit
            })

        elif record_type == "RECORD_TYPE_DISCHARGE":
            events.append({
                'date': record_date,
                'event': 'discharge',
                'Squad': None, 'Platoon': None, 'Company': None, 'Battalion': None
            })

    if not events:
        return pd.DataFrame(columns=['date', 'event', 'Squad', 'Platoon', 'Company', 'Battalion'])

    return pd.DataFrame(events).sort_values('date').reset_index(drop=True)

# Create memberships from events
def create_memberships(events_df):
    memberships = []
    current_assignment = None

    for _, event in events_df.iterrows():
        if event['event'] == 'transfer':
            if current_assignment:
                current_assignment['end_date'] = event['date'] - timedelta(days=1)
                memberships.append(current_assignment)

            current_assignment = {
                'start_date': event['date'],
                'Squad': event['Squad'],
                'Platoon': event['Platoon'],
                'Company': event['Company'],
                'Battalion': event['Battalion'],
                'end_date': None
            }

        elif event['event'] == 'discharge':
            if current_assignment:
                current_assignment['end_date'] = event['date']
                memberships.append(current_assignment)
                current_assignment = None

    if current_assignment:
        current_assignment['end_date'] = datetime.now()
        memberships.append(current_assignment)

    return pd.DataFrame(memberships)

# Generate daily strength
def calculate_daily_strength(memberships_df):
    daily_strength = []

    for _, row in memberships_df.iterrows():
        date_range = pd.date_range(start=row['start_date'], end=row['end_date'])
        for date in date_range:
            daily_strength.append({
                'date': date,
                'Battalion': row['Battalion'],
                'Company': row['Company'],
                'Platoon': row['Platoon'],
                'Squad': row['Squad']
            })

    daily_strength_df = pd.DataFrame(daily_strength)

    daily_unit_strength = daily_strength_df.groupby(
        ['date', 'Battalion', 'Company', 'Platoon', 'Squad']
    ).size().reset_index(name='strength')

    return daily_unit_strength.sort_values(by=['date', 'Battalion', 'Company', 'Platoon', 'Squad'])

# Main function
def main(input_folder, output_csv):
    all_memberships = []
    json_files = [f for f in os.listdir(input_folder) if f.endswith('.json')]
    total_files = len(json_files)

    for idx, filename in enumerate(json_files, start=1):
        filepath = os.path.join(input_folder, filename)
        data = load_json(filepath)
        events_df = extract_events(data)

        if events_df.empty:
            continue

        memberships_df = create_memberships(events_df)
        all_memberships.append(memberships_df)

        if idx % 100 == 0 or idx == total_files:
            print(f"Processed {idx}/{total_files} files...")

    if not all_memberships:
        print("No relevant records found in any JSON files.")
        return

    combined_memberships_df = pd.concat(all_memberships, ignore_index=True)
    daily_unit_strength = calculate_daily_strength(combined_memberships_df)

    daily_unit_strength.to_csv(output_csv, index=False)
    print(f"Daily unit strength history exported to {output_csv}")

# Example usage
if __name__ == "__main__":
    input_folder = r'c:\Users\micah\Projects\Retention\Milpacs'
    output_csv = 'daily_unit_strength_history.csv'
    main(input_folder, output_csv)
