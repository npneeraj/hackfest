import json
import csv
import re

def load_data(transactions_file, sanctioned_countries, all_countries):
    """Load transactions, sanctioned countries, and all countries."""
    with open(transactions_file, 'r') as f:
        data = json.load(f)
        transactions = data['transactions']  # Access the list inside the dictionary

    # with open(transactions_file, 'r') as f:
    #     transactions = json.load(f)
    
    sanctioned_countries = set(sanctioned_countries)
    all_countries = set(all_countries)
    
    return transactions, sanctioned_countries, all_countries


def check_country(address, sanctioned_countries, all_countries):
    """Extract country from address based on all_countries."""
    
    # for country in sanctioned_countries:
    #     if country.lower() in address.lower():
    #         print(f"Address: {address}, Detected Country: {country}")
    #         return country
    # return None
    address_parts = re.split(r',|;', address)
    for part in reversed(address_parts):
        part = part.strip()
        if part in sanctioned_countries:
            print(f"Address: {address}, Detected Country: {part}")
            return part
        elif part not in all_countries and part not in sanctioned_countries:
            return part
    return None


def process_transactions(transactions, sanctioned_countries, all_countries):
    """Process transactions and classify them."""
    flagged_transactions = []
    transactions_for_review = []
    
    for transaction in transactions:
        print(f"Transaction: {transaction}")
        sender_country = check_country(transaction['sender_address'], sanctioned_countries,all_countries)
        receiver_country = check_country(transaction['receiver_address'], sanctioned_countries,all_countries)
        
        if sender_country in sanctioned_countries or receiver_country in sanctioned_countries:
            flagged_transactions.append(transaction)
        elif sender_country not in all_countries or receiver_country not in all_countries:
           transactions_for_review.append(transaction)
        elif sender_country is None or receiver_country is None:
            continue

    
    return flagged_transactions, transactions_for_review


def write_csv(file_path, transactions, fields):
    """Write transactions to a CSV file."""
    with open(file_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(transactions)


def main():
    # Input files
    transactions_file = 'transactions.json'
    sanctioned_countries = ['Iran', 'North Korea', 'Syria', 'Cuba','Norway','India']
    all_countries = ['Los Angeles','United States', 'Canada', 'Iran', 'North Korea', 'Syria', 'Cuba', 'Germany', 'France', 'India']
    
    # Load data
    transactions, sanctioned_countries, all_countries = load_data(transactions_file, sanctioned_countries, all_countries)
    
    # Process transactions
    flagged_transactions, transactions_for_review = process_transactions(transactions, sanctioned_countries, all_countries)
    
    # Output files
    flagged_file = 'flagged_transactions.csv'
    review_file = 'flagged_transaction_for_review.csv'
    
    fields = ['transaction_id', 'sender_account','sender_routing_number','sender_name','sender_address',
            'receiver_account','receiver_routing_number','receiver_name', 'receiver_address', 'transaction_amount']
       
    # Write outputs
    write_csv(flagged_file, flagged_transactions, fields)
    write_csv(review_file, transactions_for_review, fields)
    
    print(f"Flagged transactions saved to {flagged_file}")
    print(f"Transactions for review saved to {review_file}")


if __name__ == '__main__':
    main()
