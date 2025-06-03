import xarray as xr
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import herbie
import pandas as pd
import warnings
from xarray.coding.times import SerializationWarning
import time

warnings.filterwarnings("ignore", category=SerializationWarning)

base_url = "https://nomads.ncep.noaa.gov/dods/gfs_0p25_1hr"


def validate_date(date_str):
    """Validate the date string format."""
    try:
        datetime.strptime(date_str, "%Y-%m-%d %H")
        return True
    except ValueError:
        raise ValueError("Date must be in format 'YYYY-MM-DD HH'")


def get_history(date, retries=3, sleep_seconds=10):
    """
    Get historical weather data for the specified date.

    Args:
        date (str): Date in format 'YYYY-MM-DD HH'
        retries (int): Number of retry attempts for failed operations
        sleep_seconds (int): Seconds to wait between retries

    Returns:
        xarray.Dataset: Historical weather data
    """
    if not validate_date(date):
        raise ValueError("Invalid date format")

    current_date = datetime.strptime(date, "%Y-%m-%d %H")
    current_date = current_date.replace(hour=0, minute=0, second=0, microsecond=0)
    previous_run = current_date - timedelta(hours=32)
    previous_run_str = previous_run.strftime("%Y%m%d")
    previous_url = f"{base_url}/gfs{previous_run_str}/gfs_0p25_1hr_18z"

    # Retry logic for initial dataset loading
    for attempt in range(retries):
        try:
            ds_previous = xr.open_dataset(previous_url, decode_times=True)
            break  # Exit loop if successful
        except OSError as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                print(f"Retrying in {sleep_seconds} seconds...")
                time.sleep(sleep_seconds)
            else:
                raise RuntimeError(
                    f"Failed to load dataset after {retries} attempts: {e}"
                )

    # Subset and process
    try:
        ds_previous = ds_previous.sel(
            time=slice(current_date - timedelta(hours=32), current_date),
            lat=slice(15, 72),
            lon=slice(190, 310),
        )
        ds_previous = ds_previous[["apcpsfc", "gustsfc", "ugrd10m", "vgrd10m"]]

        # Convert lon to -180 to 180 and rename
        ds_previous = ds_previous.assign_coords(
            lon=(ds_previous.lon + 180) % 360 - 180
        ).sortby("lon")
        ds_previous = ds_previous.rename({"lon": "longitude", "lat": "latitude"})
    except Exception as e:
        raise RuntimeError(f"Error processing dataset: {e}")

    # Retry logic for loading data
    for attempt in range(retries):
        try:
            ds_previous.load()
            break  # Success
        except Exception as e:
            print(f"❌ Load attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                print(f"Retrying load in {sleep_seconds} seconds...")
                time.sleep(sleep_seconds)
            else:
                raise RuntimeError(f"Failed to load data after {retries} attempts: {e}")

    print("cleaning history")
    other_vars = ["gustsfc", "ugrd10m", "vgrd10m"]
    ds_precip = ds_previous["apcpsfc"].sel(
        time=slice(current_date - timedelta(hours=32), current_date)
    )
    ds_all = ds_previous[other_vars].sel(
        time=slice(current_date - timedelta(hours=24), current_date)
    )
    ds_precip = ds_precip.rolling(time=6, min_periods=6).sum()
    ds_all["acc_precip"] = ds_precip.sel(time=ds_all.time)

    return ds_all


def get_future(date, retries=3, sleep_seconds=10):
    """
    Get future weather forecast data for the specified date.

    Args:
        date (str): Date in format 'YYYY-MM-DD HH'
        retries (int): Number of retry attempts for failed operations
        sleep_seconds (int): Seconds to wait between retries

    Returns:
        xarray.Dataset: Future weather forecast data
    """
    if not validate_date(date):
        raise ValueError("Invalid date format")

    current_date = datetime.strptime(date, "%Y-%m-%d %H")
    current_date = current_date.replace(hour=0, minute=0, second=0, microsecond=0)
    previous_run = current_date - timedelta(hours=6)
    previous_run_str = previous_run.strftime("%Y%m%d")
    previous_url = f"{base_url}/gfs{previous_run_str}/gfs_0p25_1hr_18z"

    # Retry logic for initial dataset loading
    for attempt in range(retries):
        try:
            print(previous_url)
            ds_future = xr.open_dataset(previous_url, decode_times=True)
            break  # Exit loop if successful
        except OSError as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                print(f"Retrying in {sleep_seconds} seconds...")
                time.sleep(sleep_seconds)
            else:
                raise RuntimeError(
                    f"Failed to load dataset after {retries} attempts: {e}"
                )

    # Subset and process
    try:
        ds_future = ds_future.sel(
            time=slice(
                current_date - timedelta(hours=6),
                current_date + timedelta(hours=24 * 4),
            ),
            lat=slice(15, 72),  # from north to south (descending if needed)
            lon=slice(190, 310),
        )
        ds_future = ds_future[["apcpsfc", "gustsfc", "ugrd10m", "vgrd10m"]]
        ds_future = ds_future.assign_coords(
            lon=(ds_future.lon + 180) % 360 - 180
        ).sortby("lon")
        ds_future = ds_future.rename({"lon": "longitude", "lat": "latitude"})
    except Exception as e:
        raise RuntimeError(f"Error processing dataset: {e}")

    # Retry logic for loading data
    for attempt in range(retries):
        try:
            ds_future.load()
            break  # Success
        except Exception as e:
            print(f"❌ Load attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                print(f"Retrying load in {sleep_seconds} seconds...")
                time.sleep(sleep_seconds)
            else:
                raise RuntimeError(f"Failed to load data after {retries} attempts: {e}")

    ds_precip = ds_future["apcpsfc"].sel(
        time=slice(
            current_date - timedelta(hours=6), current_date + timedelta(hours=24 * 4)
        )
    )
    other_vars = ["gustsfc", "ugrd10m", "vgrd10m"]
    ds_all = ds_future[other_vars].sel(
        time=slice(current_date, current_date + timedelta(hours=24 * 4))
    )
    ds_precip = ds_precip.rolling(time=6, min_periods=6).sum()
    ds_all["acc_precip"] = ds_precip.sel(time=ds_all.time)

    return ds_future


# def process_history(ds):
#     other_var = [,"gustsfc", "ugrd10m", "vgrd10m"
#     ds_precip = ds["apcpsfc"]
#     ds_all = ds[other_vars].sel(time=slice(current_date-timedelta(hours=24), current_date))
#     ds_precip = ds_precip.rolling(time=6, min_periods=6).sum()
#     ds_all['acc_precip'] = ds_precip.sel(time=ds_all.time)


if __name__ == "__main__":
    sites_df = pd.read_csv("utils/sites_in_domain.csv")
    sites_df = sites_df.rename(
        columns={"Latitude": "latitude", "Longitude": "longitude"}
    ).drop(columns=["in_domain"])

    a = time.time()
    date = "2025-06-01 22:00"
    ds_history = get_history(date)
    ds_history.to_netcdf("history.nc")

    b = time.time()
    print(f"Time taken to get history: {b - a} seconds")

    c = time.time()
    ds_future = get_future(date)
    ds_future.to_netcdf("future.nc")
    d = time.time()
    print(f"Time taken to get future: {d - c} seconds")
