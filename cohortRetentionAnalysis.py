import json
import os
import re
import pandas as pd
from datetime import datetime, timedelta

# Load JSON file
def load_json(filepath):
    with open(filepath, 'r') as f:
        return json.load(f)

# Normalize squad identifiers (letters to numbers)
def normalize_squad(squad):
    if squad is None:
        return None
    squad = squad.strip().upper()
    if len(squad) == 1 and squad.isalpha():
        return str(ord(squad) - ord('A') + 1)
    elif squad.isdigit():
        return squad
    return None

# Parse unit strings into structured data based on acceptable formats
def parse_unit(title):
    battalion_pattern = re.compile(r'(1-7|2-7|3-7|ACD)')
    company_pattern = re.compile(r'\b([A-Z])\b')
    platoon_pattern = re.compile(r'\b(\d)\b')
    squad_pattern = re.compile(r'\b([A-Z]|\d)\b')

    parts = title.split('/')

    squad = normalize_squad(next(iter(squad_pattern.findall(parts[0])), None))
    platoon = next(iter(platoon_pattern.findall(parts[1])), None) if len(parts) > 1 else None
    company = next(iter(company_pattern.findall(parts[2])), None) if len(parts) > 2 else None
    battalion = next(iter(battalion_pattern.findall(parts[3])), None) if len(parts) > 3 else None

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

# Assign cohorts based on join date
def assign_cohort(join_date, freq='M'):
    return join_date.to_period(freq).strftime('%Y-%m')

# Calculate retention based on memberships DataFrame
def calculate_retention(memberships_df, intervals=[30, 90, 180, 365]):
    cohorts = []

    memberships_df['start_date'] = pd.to_datetime(memberships_df['start_date'])
    memberships_df['end_date'] = pd.to_datetime(memberships_df['end_date'])

    memberships_df['cohort'] = memberships_df['start_date'].apply(assign_cohort)

    grouped = memberships_df.groupby(['cohort', 'Battalion', 'Company', 'Platoon', 'Squad'])

    for (cohort, battalion, company, platoon, squad), group in grouped:
        cohort_info = {
            'Cohort': cohort,
            'Battalion': battalion,
            'Company': company,
            'Platoon': platoon,
            'Squad': squad,
            'Total Members': group['start_date'].count()
        }

        for interval in intervals:
            check_date = group['start_date'].min() + timedelta(days=interval)
            retained = group[group['end_date'] >= check_date].shape[0]
            cohort_info[f'Retention @ {interval} days'] = round((retained / cohort_info['Total Members']) * 100, 2)

        cohorts.append(cohort_info)

    return pd.DataFrame(cohorts)

# Main function to process files and calculate retention
def main(input_folder, output_csv):
    all_memberships = []

    json_files = [f for f in os.listdir(input_folder) if f.endswith('.json')]

    for filename in json_files:
        filepath = os.path.join(input_folder, filename)
        data = load_json(filepath)
        events_df = extract_events(data)
        if events_df.empty:
            continue
        memberships_df = create_memberships(events_df)
        all_memberships.append(memberships_df)

    combined_memberships_df = pd.concat(all_memberships, ignore_index=True)
    retention_df = calculate_retention(combined_memberships_df)

    retention_df.to_csv(output_csv, index=False)
    print(f"Cohort retention analysis exported to {output_csv}")

# Example usage
if __name__ == "__main__":
    input_folder = r'c:\Users\micah\Projects\7Cav_Retention\Milpacs'
    output_csv = 'cohort_retention_analysis.csv'
    main(input_folder, output_csv)
