import csv
import os

def print_csv_structure(file_path):
    print(f"Reading {file_path}...")
    try:
        with open(file_path, 'r', newline='') as csvfile:
            reader = csv.reader(csvfile)
            header = next(reader)
            print(f"Columns: {header}")
            
            # Print first 5 rows
            print("\nSample data (first 5 rows):")
            for i, row in enumerate(reader):
                if i < 5:
                    print(row)
                else:
                    break
                    
            # Go back to the beginning and count rows
            csvfile.seek(0)
            next(reader)  # Skip header
            row_count = sum(1 for row in reader)
            print(f"\nTotal rows: {row_count}")
    except Exception as e:
        print(f"Error reading {file_path}: {e}")

# List all files in the data directory
print("Files in data directory:")
for file in os.listdir('data'):
    print(f"- {file}")

print("\n" + "="*80 + "\n")

# Read products.csv
print_csv_structure('data/products.csv')

print("\n" + "="*80 + "\n")

print("Note: Cannot read Excel files with built-in modules. Please install pandas and openpyxl to read the Excel files.")
