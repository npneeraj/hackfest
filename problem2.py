import json
import pandas as pd
import xmltodict
from fuzzywuzzy import fuzz, process

# Load Transaction Data (JSON)
def load_transaction_data(json_file):
    with open(json_file, 'r') as file:
        return json.load(file)

# Load Blacklist Data (XML)
def load_blacklist_data(xml_file):
    with open(xml_file, 'r') as file:
        return xmltodict.parse(file.read())

# Normalize Names for Matching
def normalize_name(name):
    return ' '.join(name.lower().strip().split())

# Match Names with Fuzzy Matching (Primary Name Only)
def is_match(name, blacklist_primary_names, threshold=85):
    match, score = process.extractOne(name, blacklist_primary_names)
    return score >= threshold

# Detect Flagged Transactions
def detect_flagged_transactions(transactions, blacklist):
    # Extract only primary names from the blacklist
    blacklist_primary_names = [normalize_name(entry['name']) for entry in blacklist]

    flagged_transactions = []
    for transaction in transactions:
        sender_name = normalize_name(transaction['sender']['name'])
        receiver_name = normalize_name(transaction['receiver']['name'])

        # Match only with primary names from the blacklist
        if is_match(sender_name, blacklist_primary_names) or is_match(receiver_name, blacklist_primary_names):
            flagged_transactions.append(transaction)

    return flagged_transactions

# Save Flagged Transactions to CSV
def save_to_csv(flagged_transactions, output_file):
    df = pd.DataFrame(flagged_transactions)
    df.to_csv(output_file, index=False)

# Main Function
def main(transaction_json, blacklist_xml, output_csv):
    # Load data
    transactions = load_transaction_data(transaction_json)
    blacklist_data = load_blacklist_data(blacklist_xml)

    # Extract blacklist entries
    blacklist = blacklist_data['blacklist']['individual']  # Assuming this structure

    # Detect flagged transactions (primary name matching only)
    flagged_transactions = detect_flagged_transactions(transactions, blacklist)

    # Save results
    save_to_csv(flagged_transactions, output_csv)
    print(f"Flagged transactions saved to {output_csv}")

# Example Usage
if __name__ == "__main__":
    main('transactions.json', 'blacklist.xml', 'flagged_transactions.csv')
