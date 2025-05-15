"""
This script converts Excel files to CSV files.
"""

import pandas as pd


def convert_excel_to_csv(excel_file_path: str, csv_file_path: str) -> None:
    """
    Converts an Excel file to a CSV file.

    Args:
        excel_file_path: Path to the Excel file
        csv_file_path: Path to the CSV file
    """
    df = pd.read_excel(excel_file_path)
    df.to_csv(csv_file_path, index=False)
    print(f"\nConverted {excel_file_path} to {csv_file_path}\n")


# Run the script if executed directly
if __name__ == "__main__":
    convert_excel_to_csv("data/orders_apr_2025.xlsx", "data/orders_apr_2025.csv")
    convert_excel_to_csv(
        "data/order_items_apr_2025.xlsx", "data/order_items_apr_2025.csv"
    )
