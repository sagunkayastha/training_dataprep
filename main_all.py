import os
import json
import shutil
from src.run_all import Prepare_Dataset


def load_categories(all_categories_path):
    """Load categories from categories_all.json"""
    # categories_path = os.path.join(
    #     os.path.dirname(os.path.dirname(__file__)), "data_all", "categories_all.json"
    # )

    with open(all_categories_path, "r") as f:
        return json.load(f)


def update_config(category_code):
    """Update config.json with new category code"""
    # config_path = os.path.join(
    #     os.path.dirname(os.path.dirname(__file__)), "config.json"
    # )
    config_path = "config.json"

    # Read current config
    with open(config_path, "r") as f:
        config = json.load(f)

    # Update category code
    config["category_code"] = category_code

    # Write back to config
    with open(config_path, "w") as f:
        json.dump(config, f, indent=4)


def main(config_path):
    # Load all categories
    with open(config_path, "r") as f:
        config = json.load(f)

    all_category_path = config["paths"]["all_category_path"]

    categories = load_categories(all_category_path)

    # Sort categories by level for organized processing
    categories.sort(key=lambda x: x["Level"])

    print(f"Found {len(categories)} categories to process")

    # Process each category
    for category in categories:
        category_code = category["CategoryCode"]
        level = category["Level"]
        description = category["Description"]
        common_name = category["CommonName"] if category["CommonName"] else "N/A"

        print(f"\n{'=' * 50}")
        print(f"Processing category: {category_code}")
        print(f"Level: {level}")
        print(f"Description: {description}")
        print(f"Common Name: {common_name}")
        print(f"{'=' * 50}")

        update_config(category_code)

        # Create and run dataset preparation
        dataset = Prepare_Dataset()
        dataset.run(new_data=config["new_data"])

        print(f"Successfully processed {category_code}")

        # try:
        #     # Update config with new category code
        #     update_config(category_code)

        #     # Create and run dataset preparation
        #     dataset = Prepare_Dataset()
        #     dataset.run(new_data=config["new_data"])

        #     print(f"Successfully processed {category_code}")

        # except Exception as e:
        #     print(f"Error processing {category_code}: {str(e)}")
        #     continue


if __name__ == "__main__":
    main("config.json")
