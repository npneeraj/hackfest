import csv
import re
import ijson
from tqdm import tqdm
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def check_country(address, sanctioned_countries, all_countries):
    """Extract country from address based on all_countries."""
    address_parts = re.split(r',|;', address)
    for part in reversed(address_parts):
        part = part.strip()
        if part in sanctioned_countries:
            logging.info(f"Address: {address}, Detected Country: {part}")
            return part
        elif part not in all_countries:
            return part  # Return unmatched country for review
    return None

def process_large_json_file(file_path, sanctioned_countries, all_countries, chunk_size=1000):
    """Process the large JSON file and classify transactions incrementally."""
    flagged_transactions_count = 0
    transactions_for_review_count = 0
    flagged_chunk = []
    review_chunk = []

    # Define the fields to include in the output
    output_fields = ['transaction_id', 'sender_name', 'receiver_name', 'sender_address', 'receiver_address', 'amount']

    # Open the output CSV files for flagged and review transactions
    with open('flagged_transactions.csv', 'w', newline='', encoding='utf-8') as flagged_file, \
         open('transactions_for_review.csv', 'w', newline='', encoding='utf-8') as review_file:

        # Initialize CSV writers
        flagged_writer = csv.DictWriter(flagged_file, fieldnames=output_fields)
        review_writer = csv.DictWriter(review_file, fieldnames=output_fields)

        # Write headers
        flagged_writer.writeheader()
        review_writer.writeheader()

        # Stream the JSON file with ijson for incremental parsing
        with open(file_path, 'r', encoding='utf-8') as f:
            transactions = ijson.items(f, 'transactions.item')  # 'transactions.item' assumes 'transactions' is an array in JSON
            for idx, transaction in tqdm(enumerate(transactions, start=1), desc="Processing transactions", unit="transaction"):
                # Extract sender and receiver countries
                sender_country = check_country(transaction['sender_address'], sanctioned_countries, all_countries)
                receiver_country = check_country(transaction['receiver_address'], sanctioned_countries, all_countries)

                # Prepare a filtered transaction for output
                filtered_transaction = {
                    'transaction_id': transaction['transaction_id'],
                    'sender_name': transaction['sender_name'],
                    'receiver_name': transaction['receiver_name'],
                    'sender_address': transaction['sender_address'],
                    'receiver_address': transaction['receiver_address'],
                    'amount': transaction['transaction_amount']
                }

                # Classify the transaction
                if sender_country in sanctioned_countries or receiver_country in sanctioned_countries:
                    flagged_transactions_count += 1
                    flagged_chunk.append(filtered_transaction)
                elif sender_country not in all_countries or receiver_country not in all_countries:
                    transactions_for_review_count += 1
                    review_chunk.append(filtered_transaction)

                # Write chunks to files when the chunk size is reached
                if len(flagged_chunk) >= chunk_size:
                    flagged_writer.writerows(flagged_chunk)
                    flagged_chunk.clear()

                if len(review_chunk) >= chunk_size:
                    review_writer.writerows(review_chunk)
                    review_chunk.clear()

            # Write any remaining transactions after processing
            if flagged_chunk:
                flagged_writer.writerows(flagged_chunk)
            if review_chunk:
                review_writer.writerows(review_chunk)

    # Log summary
    logging.info(f"Flagged transactions count: {flagged_transactions_count}")
    logging.info(f"Transactions for review count: {transactions_for_review_count}")

def main():
    # File paths
    transactions_file = r'C:\Users\Administrator\Desktop\neeraj\problem1\transactions.json'
    sanctioned_countries_file = r'C:\Users\Administrator\Desktop\neeraj\problem1\countries_blacklisted.txt'
    all_countries_file = r'C:\Users\Administrator\Desktop\neeraj\problem1\countries_all.txt'

    # Load sanctioned countries and all countries from files
    with open(sanctioned_countries_file, 'r') as f:
        sanctioned_countries = set(line.strip() for line in f.readlines())

    with open(all_countries_file, 'r') as f:
        all_countries = set(line.strip() for line in f.readlines())

    # Process the transactions
    try:
        process_large_json_file(transactions_file, sanctioned_countries, all_countries)
        logging.info("Processing complete.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == '__main__':
    main()
