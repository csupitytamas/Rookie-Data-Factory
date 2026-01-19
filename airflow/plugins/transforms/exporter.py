import os
from pathlib import Path
import pandas as pd
import yaml
import openpyxl


def export_data(df: pd.DataFrame, table_name: str, file_format: str):
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

        elif file_format == "txt":
            df.to_csv(file_path, sep="\t", index=False)

        elif file_format == "xml":
            df.to_xml(file_path, index=False)

        elif file_format == "yaml":
            # YAML-be minden sort külön dict-ként listázunk
            yaml_data = df.to_dict(orient="records")
            with open(file_path, "w", encoding="utf-8") as f:
                yaml.dump(yaml_data, f, allow_unicode=True)

        else:
            print(f"⚠️ Nem támogatott fájlformátum: {file_format}")
            return

        print(f"✅ Fájl mentve: {file_path}")

    except Exception as e:
        print(f"❌ Hiba a fájl exportálás során: {e}")