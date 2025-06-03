import pandas as pd
import numpy as np


class CleanData:
    def __init__(self, window=7, min_valid=5, samples_per_site=50):
        self.sites_df = None
        self.category_df = None
        self.window = window
        self.min_valid = min_valid
        self.samples_per_site = samples_per_site

    def load_data(self):
        # Drop only rows where PPM3 is negative (keep NaNs and non-negative values)
        self.category_df = self.category_df[
            (self.category_df["PPM3"] >= 0) | (self.category_df["PPM3"].isna())
        ]
        filtered_rows = []
        for site_id in self.sites_df["Id"]:
            subset = self.category_df[self.category_df["SiteId"] == site_id]
            if len(subset) > 0:
                filtered_rows.append(subset)

        self.result_df = pd.concat(filtered_rows, ignore_index=True)
        self.grouped = self.result_df.groupby(["SiteId"])
        all_groups = list(self.grouped.groups.keys())

        # Find sites with sufficient number of values based on config
        sites_with_sufficient_data = []
        for group in all_groups:
            site_data = self.grouped.get_group(group).sort_values(by="Starting")
            if len(site_data) > self.samples_per_site:
                sites_with_sufficient_data.append(
                    {"SiteId": group, "Count": len(site_data)}
                )

        # Convert to DataFrame
        sites_count_df = pd.DataFrame(sites_with_sufficient_data)
        self.valid_sites = sites_count_df["SiteId"].tolist()

    def station_fill(self, group):
        df = self.grouped.get_group(group).sort_values(by="Starting")
        df["Starting"] = pd.to_datetime(df["Starting"])

        # endpoints
        start = df["Starting"].min()
        end = df["Starting"].max()

        # build full daily frame
        all_days = pd.DataFrame({"Starting": pd.date_range(start, end, freq="D")})

        # merge: keeps every day in all_days, brings in your original columns, NaN where missing
        df_full = all_days.merge(df, on="Starting", how="left")

        # make sure Starting is datetime and set it as your index
        df_full["Starting"] = pd.to_datetime(df_full["Starting"])
        ts = df_full.set_index("Starting")

        return ts

    def remove_unrecoverable_nans(self, df, column="PPM3", window_d=7, min_valid=4):
        # 1) ensure full daily index
        full_idx = pd.date_range(df.index.min(), df.index.max(), freq="D")
        df = df.reindex(full_idx)

        # 2) 0/1 series of where we actually have data
        has_obs = df[column].notna().astype(int)

        # 3) count non-nulls in a window of -7 days to +1 day
        counts = has_obs.rolling(
            window=window_d + 1,
            center=False,
            min_periods=1,
        ).sum()

        # 4) keep only those timestamps with enough neighbors
        df = df.loc[counts >= min_valid]

        # 5) Count zeros in rolling window and remove periods with too many zeros
        zero_mask = df[column] == 0
        zero_counts = zero_mask.rolling(
            window=window_d + 1, center=False, min_periods=1
        ).sum()

        # Remove periods where more than 3 values in the window are zero
        df = df[zero_counts <= 3]

        return df

    def clean_station(self):
        all_dfs = []
        for group in self.valid_sites:
            ts = self.station_fill(group)
            ts = self.remove_unrecoverable_nans(ts, "PPM3", self.window, self.min_valid)
            if len(ts) == 0:
                print(f"No data found for group {group}")
                continue
            # Skip if less than 50 data points after cleaning
            if len(ts) < 40:
                print(
                    f"Skipping group {group} - only {len(ts)} data points after cleaning"
                )
                continue
            # Get the unique non-NaN SiteId
            site_id = ts["SiteId"].dropna().unique()[0]
            # Fill NaNs with it
            ts["SiteId"] = ts["SiteId"].fillna(site_id)
            ts["Date"] = ts.index
            all_dfs.append(ts)

        if not all_dfs:
            print("No sites with sufficient data points after cleaning")
            return None

        all_dfs = pd.concat(all_dfs, ignore_index=True)
        return all_dfs

    def run(self, sites_df, category_df):
        self.sites_df = sites_df
        self.category_df = category_df
        self.load_data()
        all_clean_data = self.clean_station()
        return all_clean_data


if __name__ == "__main__":
    sites_df = pd.read_csv(r"files\sites_in_domain.csv")
    category_df = pd.read_csv(r"files\CUP_df.csv")
    clean_data = CleanData()
    clean_data.run(sites_df, category_df)
