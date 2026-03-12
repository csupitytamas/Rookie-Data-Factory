import os
from pathlib import Path
import pandas as pd
import yaml
import openpyxl
def export_data(df: pd.DataFrame, table_name: str, file_format: str, output_path: str = None):
    if output_path:
        output_dir = Path(output_path)
    else:
        output_dir = Path(__file__).parent.parent / "out" / "output"
    try:
        os.makedirs(output_dir, exist_ok=True)
    except OSError as e:
        output_dir = Path(__file__).parent.parent / "out" / "output"
        os.makedirs(output_dir, exist_ok=True)
    file_path = output_dir / f"{table_name}.{file_format.lower()}"
    try:
        if file_format == "csv":
            df.to_csv(file_path, index=False)

        elif file_format == "json":
            df.to_json(file_path, orient="records", indent=2)

        elif file_format == "parquet":
            df.to_parquet(file_path, index=False)

        elif file_format == "excel" or file_format == "xlsx":
            df.to_excel(file_path, index=False)


        print(f"File saved to: {file_path}")

    except Exception as e:
        print(f"Error during file exporting: {e}")