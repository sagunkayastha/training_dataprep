from dotenv import load_dotenv
import os
import pandas as pd
import urllib.parse
import json

from .Get_sites_in_domain import HRRRDomainChecker
from .query_category import CategoryDataFetcher
from .clean_data import CleanData
from .combine_met_ppm3 import MetDataCombiner
# from upload_to_aws import *


class Prepare_Dataset:
    def __init__(self, category_code=None):
        self._get_db_config()
        self.load_config()
        self.conn_str = self.server_connection()

        self.training_data_dir = self.config["directories"]["data_root"]
        self.sites_in_domain_path = self.config["paths"]["sites_in_domain_path"]
        self.category = self.config["category_code"]
        self.category_dir = os.path.join(self.training_data_dir, self.category)
        os.makedirs(self.training_data_dir, exist_ok=True)
        os.makedirs(self.category_dir, exist_ok=True)

        # Load libraries
        self.domain_checker = HRRRDomainChecker(self.config)
        self.category_fetcher = CategoryDataFetcher(
            self.config, self.category, self.training_data_dir
        )
        self.clean_data = CleanData(
            window=self.config.get("window", 24),
            min_valid=self.config.get("min_valid", 6),
            samples_per_site=self.config.get("samples_per_site", 1000),
        )

        self.met_combiner = MetDataCombiner(
            output_dir=os.path.join(self.training_data_dir, self.category, "site_data"),
            met_dir=self.config["paths"]["met_path"],
        )

        self.sites_in_domain = None
        self.category_df = None

    def _get_db_config(self):
        load_dotenv()
        self.server = os.getenv("DB_SERVER")
        self.username = os.getenv("DB_USERNAME")
        self.password = os.getenv("DB_PASSWORD")
        self.database = os.getenv("DB_NAME")

    def load_config(self):
        """Load category_code from config.json"""
        try:
            config_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)), "config.json"
            )
            with open(config_path, "r") as f:
                config = json.load(f)
                self.config = config
                self.final_columns = config.get("final_columns", [])
                self.daily_columns = config.get("daily_columns", [])

        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(
                f"Error loading config.json: {str(e)}. Using default category_code: CUP"
            )
            return "CUP"

    def server_connection(self):
        # URL encode the password to handle special characters

        encoded_password = urllib.parse.quote_plus(self.password)

        # Create SQLAlchemy connection string
        conn_str = (
            f"mssql+pyodbc://{self.username}:{encoded_password}@{self.server}/{self.database}"
            "?driver=ODBC+Driver+17+for+SQL+Server"
            "&TrustServerCertificate=yes"
            "&Connection+Timeout=30"
            "&Encrypt=yes"
        )
        # print(conn_str)
        # exit()
        return conn_str

    def get_sites_in_domain(self):
        if os.path.exists(self.sites_in_domain_path):
            print("Loading sites in domain from file")
            self.sites_in_domain = pd.read_csv(self.sites_in_domain_path)
        else:
            print("Getting sites in domain")
            self.sites_in_domain = self.domain_checker.run(self.conn_str)
            self.sites_in_domain.to_csv(self.sites_in_domain_path, index=False)

    def run(self, new_data=False):
        self.get_sites_in_domain()

        if new_data:
            print("Fetching new category data")
            self.category_df = self.category_fetcher.run(
                self.conn_str, self.sites_in_domain
            )
        else:
            print("Loading data from category file")
            self.category_df = pd.read_csv(
                os.path.join(self.category_dir, f"{self.category}_df.csv")
            )

        if self.sites_in_domain is not None and self.category_df is not None:
            print("Cleaning data")

            clean_data_o = self.clean_data.run(self.sites_in_domain, self.category_df)

            # Drop columns
            columns_to_drop = [
                "VariantCode",
                "Interval",
                "Count",
            ]
            clean_data_o = clean_data_o.drop(columns=columns_to_drop)
            clean_data_o.to_csv(
                os.path.join(self.category_dir, f"clean_data_{self.category}.csv"),
                index=False,
            )
        else:
            print("Loading data from files and cleaning data")
            self.sites_in_domain = pd.read_csv(self.sites_in_domain_path)
            self.category_df = pd.read_csv(
                os.path.join(self.category_dir, f"{self.category}_df.csv")
            )
            self.clean_data = self.clean_data.run(
                self.sites_in_domain, self.category_df
            )
            self.clean_data.to_csv(
                os.path.join(self.category_dir, f"clean_data_{self.category}.csv"),
                index=False,
            )

        # Combine met and clean data
        clean_data_path = os.path.join(
            self.category_dir, f"clean_data_{self.category}.csv"
        )

        print("Combining met and clean data")
        self.met_combiner.process_data(clean_data_path, self.daily_columns)


if __name__ == "__main__":
    obj = Prepare_Dataset()
    obj.run(new_data=False)
