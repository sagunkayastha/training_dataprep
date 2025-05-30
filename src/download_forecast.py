import pandas as pd
import warnings
import xarray as xr
from datetime import datetime, timedelta
import shutil
from herbie import Herbie
import time

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)


class WeatherDataFetcher:
    """A class to fetch and process weather data using Herbie."""

    def __init__(self, sites_file: str):
        """
        Initialize the WeatherDataFetcher.

        Args:
            sites_file (str): Path to CSV file containing site coordinates
        """
        self.sites_file = sites_file
        self.sites_df = self._load_sites()

    def _load_sites(self) -> pd.DataFrame:
        """Load and prepare site coordinates from CSV file."""
        sites_df = pd.read_csv(self.sites_file)
        return sites_df.rename(
            columns={"Latitude": "latitude", "Longitude": "longitude"}
        ).drop(columns=["in_domain"], errors="ignore")

    def _get_herbie_data(
        self, date_obj: datetime, is_precip: bool = False
    ) -> xr.Dataset:
        """
        Get data from Herbie for a specific datetime.

        Args:
            date_obj (datetime): The datetime to fetch data for
            is_precip (bool): Whether this is for precipitation data

        Returns:
            xr.Dataset: The fetched weather data
        """
        model = "gfs"
        product= "pgrb2.0p25"
        if is_precip:
            # For precipitation, use 6-hour forecast from 6 hours earlier
            precip_date = date_obj - timedelta(hours=6)
            H = Herbie(
                precip_date.strftime("%Y-%m-%d %H:%M:%S"),
                model=model,
                product=product,
                fxx=6,
                save_dir="./data",
                verbose=False,
                priority=[ "aws"],
            )
            H.download()
            r_var = H.xarray(search=":APCP:surface:")
            
            del H
            return r_var
        else:
            # For other variables, use analysis (fxx=0)
            H = Herbie(
                date_obj.strftime("%Y-%m-%d %H:%M:%S"),
                model=model,
                product=product,
                fxx=0,
                save_dir="./data",
                verbose=False,
                priority=["aws"],
            )
            variables = ":GUST:surface"
            H.download(search=variables)
            hr_ds = H.xarray(variables, remove_grib=False)
            # return xr.merge(hr_ds)
            
            del H
            return hr_ds

    def _process_hourly_data(self, date_obj: datetime) -> xr.Dataset:
        """
        Process data for a single hour, combining meteorological and precipitation data.

        Args:
            date_obj (datetime): The datetime to process

        Returns:
            xr.Dataset: Combined dataset for this hour
        """
        # Get meteorological data
        met_data = self._get_herbie_data(date_obj, is_precip=False)

        # Get precipitation data
        precip_data = self._get_herbie_data(date_obj, is_precip=True)

        # Add precipitation to meteorological data
        met_data["tp"] = precip_data["tp"]

        return met_data

    def _create_time_range(self, date: str, hist: bool = True) -> pd.DatetimeIndex:
        """
        Create time range based on date and hist parameter.

        Args:
            date (str): Date string in format "%Y-%m-%d %H:%M:%S"
            hist (bool): If True, get 24 hours ending 1 hour before date.
                        If False, get 24 hours starting from date.

        Returns:
            pd.DatetimeIndex: Range of datetime objects
        """
        current = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")

        if hist:
            return pd.date_range(end=current - timedelta(hours=1), periods=24, freq="h")
        else:
            return pd.date_range(
                start=current,
                periods=96,  # Fixed typo: was "preiod"
                freq="h",
            )

    def _process_points_data(self, dataset: xr.Dataset) -> xr.Dataset:
        """
        Extract point data and clean up coordinates.

        Args:
            dataset (xr.Dataset): The dataset to process

        Returns:
            xr.Dataset: Processed dataset with point data
        """
        # Extract points using Herbie's pick_points method
        new_ds = dataset.herbie.pick_points(self.sites_df, method="nearest")

        # Clean up point IDs and coordinates
        point_ids = new_ds["point_Id"].data.astype(str)
        new_ds = new_ds.assign_coords(point_Id=("point", point_ids))
        new_ds = new_ds.swap_dims({"point": "point_Id"})

        # Keep only desired coordinates
        desired_coords = {"time", "point_Id", "point_latitude", "point_longitude"}
        coords_to_drop = [c for c in new_ds.coords if c not in desired_coords]
        new_ds = new_ds.drop_vars(coords_to_drop, errors="ignore")

        # Calculate wind speed from u and v components
        new_ds["wind_speed"] = (new_ds["u10"] ** 2 + new_ds["v10"] ** 2) ** 0.5

        return new_ds

    def get_weather_data(self, date: str, hist: bool = True) -> xr.Dataset:
        """
        Get weather data for specified date and time range.

        Args:
            date (str): Date string in format "%Y-%m-%d %H:%M:%S"
            hist (bool): If True, get 24 hours of historical data ending 1 hour before date.
                        If False, get 24 hours of forecast data starting from date.

        Returns:
            xr.Dataset: Weather dataset with variables for all sites and times
        """
        # Create time range
        time_range = self._create_time_range(date, hist)

        # Process data for each hour
        all_hourly_data = []
        for date_obj in time_range:
            hourly_data = self._process_hourly_data(date_obj)
            all_hourly_data.append(hourly_data)

        # Concatenate all hours
        combined_dataset = xr.concat(all_hourly_data, dim="time")

        # Process point data
        final_dataset = self._process_points_data(combined_dataset)
        # shutil.rmtree("data")
        return final_dataset


if __name__ == "__main__":
    sites_file = "utils/sites_in_domain.csv"
    fetcher = WeatherDataFetcher(sites_file=sites_file)

    # Get data
    date = "2025-05-27 00:00:00"
    start_time = time.time()
    weather_history = fetcher.get_weather_data(date, hist=True)
    # weather_future = fetcher.get_weather_data(date, hist=False)
    end_time = time.time()
    print(f"Time taken: {end_time - start_time} seconds")
    # print(weather_data)
