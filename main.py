import sys
from src.run_all import Prepare_Dataset
import json


def main():
    # Load config from config.json
    try:
        with open("config.json", "r") as f:
            config = json.load(f)
    except FileNotFoundError:
        print(
            "Error: config.json not found. Please create a config file with a 'category_code' key."
        )
        sys.exit(1)
    except json.JSONDecodeError:
        print("Error: config.json is not a valid JSON file.")
        sys.exit(1)

    category_code = config.get("category_code")

    new_data = config.get("new_data")
    if not category_code:
        print("Error: 'category_code' not found in config.json.")
        sys.exit(1)

    dataset = Prepare_Dataset(category_code)
    dataset.run(new_data=new_data)
    # Add additional processing steps as needed


if __name__ == "__main__":
    main()
