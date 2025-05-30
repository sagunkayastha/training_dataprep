import xarray as xr
import pandas as pd
import numpy as np
import os
import shutil
import icechunk


class MetDataCombiner:
    def __init__(self, output_dir, met_dir):
        """
        Initialize the MetDataCombiner class.

        Args:
            output_dir (str): Directory path where the combined data will be saved
        """
        self.output_dir = output_dir

        self.full_met_path = met_dir
        self._create_output_directory()

    def _create_output_directory(self):
        """Create the output directory if it doesn't exist."""
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        os.makedirs(self.output_dir, exist_ok=True)

    def _compute_wind_metrics(self, df):
        """
        Compute wind speed and direction from u10 and v10 components.

        Args:
            df (pd.DataFrame): DataFrame containing u10 and v10 columns

        Returns:
            pd.DataFrame: DataFrame with added wind_speed and wind_dir columns
        """
        df["wind_speed"] = np.sqrt(df["u10"] ** 2 + df["v10"] ** 2)
        df["wind_dir"] = (np.arctan2(-df["u10"], -df["v10"]) * 180 / np.pi) % 360
        return df

    def fill_missing_values(self, ds):  # Ensure time is datetime
        time_index = pd.to_datetime(ds.time.values)

        full_range = pd.date_range(
            start=time_index.min(), end=time_index.max(), freq="H"
        )
        ds_full = ds.reindex(time=full_range)
        return ds_full.ffill(dim="time")

    def convert_and_remove_tz(self, timestamp, target_tz):
        """
        Localizes a naive timestamp to UTC, converts it to target_tz,
        and then makes it naive again.
        """
        if pd.isna(timestamp):  # Handle potential NaN values
            return pd.NaT
        return timestamp.tz_localize("UTC").tz_convert(target_tz).tz_localize(None)

    def process_data(self, category_data_path, final_columns, end_date="2025-05-20"):
        """
        Process and combine meteorological data with cup data.

        Args:
            category_data_path (str): Path to the CUP data CSV file
            end_date (str): End date for filtering data
        """
        # Initialize icechunk repository

        # storage_config = icechunk.local_filesystem_storage(self.met_dir)
        # repo = icechunk.Repository.open(storage_config)
        # session = repo.readonly_session("main")

        # ds = xr.open_zarr(session.store, consolidated=False)
        ds = xr.open_dataset(self.full_met_path)

        ds = self.fill_missing_values(ds)
        ds = ds.assign_coords(point_Id=ds.coords["point_Id"].values.astype(str))
        # ds["acc_precip"] = ds["tp"].rolling(time=6, min_periods=1).sum()
        ds["acc_precip"] = ds["tp"]
        ds["wind_speed"] = np.sqrt(ds["u10"] ** 2 + ds["v10"] ** 2)
        # Read and process cup data
        category_data = pd.read_csv(category_data_path)
        category_data["Date"] = pd.to_datetime(category_data["Date"])
        category_data = category_data[category_data["Date"] <= pd.Timestamp(end_date)]
        category_data["Date"].values.astype("datetime64[ns]")

        # Group data by site
        grouped = category_data.groupby(["SiteId"])
        all_groups = list(grouped.groups.keys())
        category_code = category_data.dropna().CategoryCode.unique()[0]
        all_df = []

        for group in all_groups:
            print(group)
            station_data = grouped.get_group(group).copy()

            station_data.index = station_data["Date"]
            target_timezone = station_data.dropna()["Timezone"].unique()[0]
            station_data.loc[:, "LocalTime"] = station_data["Date"].apply(
                self.convert_and_remove_tz, args=(target_timezone,)
            )
            # Get meteorological data
            # print(station_data.Date.values)

            met_data = ds.sel(
                point_Id=group, time=station_data.Date.values
            ).to_dataframe()

            # Merge data
            merged_df = station_data.join(met_data, how="inner").drop(
                columns=[
                    "CategoryCode",
                    "point_Id",
                ]
            )

            merged_df["local_time"] = pd.to_datetime(merged_df["LocalTime"])
            merged_df["date"] = merged_df["local_time"].dt.floor(
                "D"
            )  # midnight of each day

            # 2. For each variable, compute daily mean and daily max via transform
            for col in [
                "PPM3",
            ]:
                merged_df[f"{col}_daily_avg"] = merged_df.groupby("date")[
                    col
                ].transform("mean")
                merged_df[f"{col}_daily_max"] = merged_df.groupby("date")[
                    col
                ].transform("max")

            # merged_df = merged_df[final_columns]
            # imputation Columns
            imputation_columns = [
                "gust",
                "t2m",
                "u10",
                "v10",
                "tp",
                "r2",
                "orog",
                "sdswrf",
            ]
            merged_df = merged_df[imputation_columns + final_columns]

            # Get site and category information
            site_id = station_data.dropna().SiteId.unique()[0]

            # Save individual site data

            output_file = os.path.join(
                self.output_dir, f"{category_code}_{site_id}.csv"
            )
            merged_df.to_csv(output_file, index=False)

            all_df.append(merged_df)

        # Combine and save all data
        combined_df = pd.concat(all_df, axis=0, ignore_index=True)
        combined_output_file = os.path.join(
            self.output_dir, f"{category_code}_full.csv"
        )
        # combined_df.to_csv(combined_output_file, index=False)

        return combined_df


if __name__ == "__main__":
    # Example usage
    output_directory = "F:\Pollensense\Clean_codes\/files\site_data"
    category_code = "CUP"
    met_dir = "files/All_met"
    combiner = MetDataCombiner(output_directory, met_dir)
    combiner.process_data("files/CUP_clean_data_tz.csv")
