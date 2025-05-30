import pandas as pd
import os
from tqdm import tqdm
import json
from utils.query import get_query
# SELECT * FROM GetGEMValue(46.34592819213867,-119.27582550048828,'2025-01-02','OA',null)


class CategoryDataFetcher:
    def __init__(self, config, category_code, training_data_dir):
        self.config = config
        self.category_code = category_code
        self.training_data_dir = training_data_dir
        self.engine = None
        self.site_df = None
        self.category_df = None

    def connect_to_database(self, conn_str):
        """Connect to the database using SQLAlchemy."""
        from sqlalchemy import create_engine

        self.engine = create_engine(conn_str)

    def load_site_data(self):
        """Load site data from CSV if available, otherwise fetch from database."""
        sites_output_path = os.path.join(self.training_data_dir, "sites_in_domain.csv")
        if os.path.exists(sites_output_path):
            self.site_df = pd.read_csv(sites_output_path)
        else:
            print("Sites file not found. Please run Get_sites_in_domain.py first.")
            return None

    def get_site_timezones(self):
        """Fetch timezone information for all sites"""
        print("Fetching Timezone information for all sites...")
        site_query = f"""
        SELECT Id, Timezone
        FROM [PollenSenseLive].[dbo].[Site]
        WHERE Id IN ({",".join([f"'{id}'" for id in self.site_df["Id"]])})
        """
        site_timezones = pd.read_sql(site_query, self.engine)
        print(f"Found Timezone information for {len(site_timezones)} sites")
        return site_timezones

    def fetch_category_data(self, site_timezones):
        """Fetch category data for all sites"""
        print("\nProcessing data for each site...")
        all_data = []

        for site_id in tqdm(self.site_df["Id"], desc="Processing sites"):
            # query = f"""
            # SELECT *
            # FROM [PollenSenseLive].[dbo].[VariantRollup]
            # WHERE CategoryCode = '{self.category_code}'
            # AND Interval = 'hour'
            # AND VariantCode = 'M'
            # AND SiteId = '{site_id}'
            # """

            # query = f"""
            # WITH ProvisionedTimes AS (
            #     SELECT
            #         DATETRUNC(WEEK, MIN([From])) AS Starting,
            #         DATEADD(WEEK, 1, DATETRUNC(WEEK, MAX([To]))) AS Ending
            #     FROM [PollenSenseLive].[dbo].[AllProvisions]
            #     WHERE SiteID = '{site_id}'
            # )
            # SELECT *
            # FROM [PollenSenseLive].[dbo].[VariantRollup] AS VR
            # WHERE VR.SiteId = '{site_id}'
            # AND VR.VariantCode = 'GS2'
            # AND VR.[Interval] = 'hour'
            # AND VR.CategoryCode = '{self.category_code}'
            # AND VR.Starting BETWEEN
            #         (SELECT Starting FROM ProvisionedTimes)
            #         AND
            #         (SELECT Ending FROM ProvisionedTimes)
            # """
            query = get_query(
                site_id, self.config["VariantCode"], "hour", self.category_code
            )

            df = pd.read_sql(query, self.engine)
            if not df.empty:
                if site_id in site_timezones["Id"].values:
                    site_timezone = site_timezones[site_timezones["Id"] == site_id][
                        "Timezone"
                    ].iloc[0]
                    df["Timezone"] = site_timezone
                else:
                    df["Timezone"] = None
                all_data.append(df)

        return all_data

    def process_and_save_data(self, all_data):
        """Process and save the fetched data"""
        if all_data:
            print("\nCombining all data...")
            self.category_df = pd.concat(all_data, ignore_index=True)
            print(f"Total rows in {self.category_code}_df: {len(self.category_df)}")
            print("\nTimezone information included in the dataset")

            print("\nSaving data to CSV...")
            category_dir = os.path.join(self.training_data_dir, self.category_code)
            os.makedirs(category_dir, exist_ok=True)
            self.category_df.to_csv(
                os.path.join(category_dir, f"{self.category_code}_df.csv"), index=False
            )
            print(f"Data saved to {self.category_code}_df.csv")
        else:
            print("No data found for any sites")
            return self.category_df

    def close_connection(self):
        """Close the database connection"""
        if self.engine:
            self.engine.dispose()

    def run(self, conn_str, site_df):
        """Main method to execute the entire process"""
        try:
            self.connect_to_database(conn_str)
            if site_df is None:
                self.load_site_data()

            else:
                self.site_df = site_df

            site_timezones = self.get_site_timezones()
            all_data = self.fetch_category_data(site_timezones)
            category_df = self.process_and_save_data(all_data)

        finally:
            self.close_connection()

        return category_df
