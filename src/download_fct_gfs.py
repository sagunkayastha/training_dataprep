import pandas as pd
import getgfs
import numpy as np
import xarray as xr
from io import StringIO
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import time


def load_site_data(sites_file):
    """Load site data from CSV file"""
    # Clean column names by removing asterisks
    df = pd.read_csv(sites_file).iloc[:20]
    df.columns = df.columns.str.replace("*", "", regex=False)
    return df


def fetch_weather_for_site(site_info, times, vars_to_fetch):
    """Fetch weather data for a single site - thread-safe version"""
    site_id, lat, lon = site_info

    # Each thread gets its own forecast object to avoid conflicts
    f = getgfs.Forecast("0p25", "1hr")

    print(
        f"[Thread {threading.current_thread().name}] Processing site {site_id} at lat={lat:.3f}, lon={lon:.3f}"
    )

    # Dictionary to store lists of values
    data_dict = {var: [] for var in vars_to_fetch}
    data_dict["datetime"] = []

    # Loop through time steps
    for dt in times:
        dt_str = dt.strftime("%Y%m%d %H:%M")
        data_dict["datetime"].append(dt)

        try:
            res = f.get(vars_to_fetch, dt_str, lat, lon)
            for var in vars_to_fetch:
                value = res.variables[var].data.squeeze()
                fill_val = np.float64(f.variables[var]["_FillValue"])
                if value == fill_val or np.isnan(value):
                    data_dict[var].append(np.nan)
                else:
                    data_dict[var].append(float(value))
        except Exception as e:
            print(
                f"  [Thread {threading.current_thread().name}] Failed for {dt_str}: {e}"
            )
            for var in vars_to_fetch:
                data_dict[var].append(np.nan)

    # Convert to DataFrame
    df = pd.DataFrame(data_dict)
    df["apcpsfc"] = df["apcpsfc"].ffill()

    # Forward-fill missing data
    # df[vars_to_fetch] = df[vars_to_fetch].ffill()

    # Compute 6-hour rolling accumulated precipitation difference
    df["acc_precip"] = df["apcpsfc"].diff(periods=6)

    # For first 6 rows (insufficient lag), use original accumulated values
    df.loc[:5, "acc_precip"] = df.loc[:5, "apcpsfc"]

    # Calculate wind speed
    df["wind_speed"] = np.sqrt(df["ugrd10m"] ** 2 + df["vgrd10m"] ** 2)

    print(
        f"  [Thread {threading.current_thread().name}] Successfully processed site {site_id}"
    )

    # Return site_id and processed data
    return site_id, {
        "datetime": df["datetime"].tolist(),
        "apcpsfc": df["apcpsfc"].values,
        "gustsfc": df["gustsfc"].values,
        "ugrd10m": df["ugrd10m"].values,
        "vgrd10m": df["vgrd10m"].values,
        "acc_precip": df["acc_precip"].values,
        "wind_speed": df["wind_speed"].values,
    }


def process_sites_parallel(site_data, times, vars_to_fetch, max_workers=16):
    """Process all sites in parallel using ThreadPoolExecutor"""

    # Prepare site information for threading
    site_info_list = []
    for idx, row in site_data.iterrows():
        site_info_list.append((row["Id"], row["Latitude"], row["Longitude"]))

    weather_data_dict = {}
    failed_sites = []

    print(
        f"Starting parallel processing with {max_workers} threads for {len(site_info_list)} sites..."
    )
    start_time = time.time()

    # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(
        max_workers=max_workers, thread_name_prefix="WeatherWorker"
    ) as executor:
        # Submit all tasks
        future_to_site = {
            executor.submit(
                fetch_weather_for_site, site_info, times, vars_to_fetch
            ): site_info[0]
            for site_info in site_info_list
        }

        # Process completed tasks
        completed = 0
        total_sites = len(site_info_list)

        for future in as_completed(future_to_site):
            site_id = future_to_site[future]
            completed += 1

            try:
                result_site_id, weather_data = future.result()
                weather_data_dict[result_site_id] = weather_data
                print(
                    f"Progress: {completed}/{total_sites} sites completed ({completed / total_sites * 100:.1f}%)"
                )

            except Exception as e:
                print(f"ERROR: Site {site_id} failed with exception: {e}")
                failed_sites.append(site_id)

    end_time = time.time()
    processing_time = end_time - start_time

    print(f"\nParallel processing completed in {processing_time:.2f} seconds")
    print(f"Successfully processed: {len(weather_data_dict)} sites")
    print(f"Failed sites: {len(failed_sites)} sites")

    if failed_sites:
        print(f"Failed site IDs: {failed_sites}")

    return weather_data_dict


def create_xarray_dataset(site_data, weather_data_dict):
    """Create xarray dataset from all site weather data"""

    # Get dimensions
    n_sites = len(site_data)
    n_times = len(weather_data_dict[site_data.iloc[0]["Id"]]["datetime"])

    # Get time coordinate from first site
    first_site_id = site_data.iloc[0]["Id"]
    time_coord = weather_data_dict[first_site_id]["datetime"]

    # Create site coordinate
    site_ids = site_data["Id"].values

    # Variables to include in dataset
    weather_vars = [
        "apcpsfc",
        "gustsfc",
        "ugrd10m",
        "vgrd10m",
        "acc_precip",
        "wind_speed",
    ]

    # Initialize data arrays
    data_vars = {}

    for var in weather_vars:
        # Create 2D array (sites x time)
        var_data = np.full((n_sites, n_times), np.nan)

        for i, site_id in enumerate(site_ids):
            if site_id in weather_data_dict:
                var_data[i, :] = weather_data_dict[site_id][var]

        data_vars[var] = (["point_Id", "time"], var_data)

    # Add site coordinates as data variables
    data_vars["latitude"] = (["point_Id"], site_data["Latitude"].values)
    data_vars["longitude"] = (["point_Id"], site_data["Longitude"].values)
    data_vars["in_domain"] = (["point_Id"], site_data["in_domain"].values)

    # Create coordinates
    coords = {"point_Id": site_ids, "time": pd.to_datetime(time_coord)}

    # Create xarray Dataset
    ds = xr.Dataset(data_vars, coords=coords)

    # Add attributes
    # ds.attrs = {
    #     "title": "Weather forecast data for multiple sites",
    #     "description": "GFS weather forecast data fetched for multiple geographic locations",
    #     "created": pd.Timestamp.now().isoformat(),
    #     "forecast_start": str(time_coord[0]),
    #     "forecast_end": str(time_coord[-1])
    # }

    # Add variable attributes
    # var_attrs = {
    #     "apcpsfc": {"long_name": "Accumulated precipitation", "units": "mm"},
    #     "gustsfc": {"long_name": "Wind gust speed", "units": "m/s"},
    #     "ugrd10m": {"long_name": "U-component wind at 10m", "units": "m/s"},
    #     "vgrd10m": {"long_name": "V-component wind at 10m", "units": "m/s"},
    #     "acc_precip": {"long_name": "6-hour precipitation", "units": "mm"},
    #     "wind_speed": {"long_name": "Wind speed magnitude", "units": "m/s"},
    #     "latitude": {"long_name": "Latitude", "units": "degrees_north"},
    #     "longitude": {"long_name": "Longitude", "units": "degrees_east"}
    # }

    # for var, attrs in var_attrs.items():
    #     if var in ds:
    #         ds[var].attrs = attrs

    return ds


def main(start_time, total_hours, max_workers=16):
    """Main function to process all sites and create xarray dataset"""

    # Load site data
    site_data = load_site_data(
        "F:/Forecasting_PS/Training_prep/utils/sites_in_domain.csv"
    )
    print(f"Loaded {len(site_data)} sites")

    # Create time range
    times = pd.date_range(start=start_time, periods=total_hours, freq="h")
    print(f"Processing {len(times)} time steps from {start_time}")

    # Variables to fetch
    vars_to_fetch = ["apcpsfc", "gustsfc", "ugrd10m", "vgrd10m"]

    # Process all sites in parallel
    weather_data_dict = process_sites_parallel(
        site_data, times, vars_to_fetch, max_workers
    )

    # Create xarray dataset
    if weather_data_dict:
        print(f"\nCreating xarray dataset with {len(weather_data_dict)} sites...")
        ds = create_xarray_dataset(site_data, weather_data_dict)

        # Save to NetCDF file
        output_file = "weather_forecast_multisite.nc"
        ds.to_netcdf(output_file)
        print(f"Dataset saved to {output_file}")

        # Display dataset info
        print("\nDataset summary:")
        print(ds)

        # Show sample data for first site
        first_site = list(weather_data_dict.keys())[0]
        print(f"\nSample data for site {first_site}:")
        print(ds.sel(point_Id=first_site).to_dataframe().head())

        return ds
    else:
        print("No weather data was successfully fetched!")
        return None


# Example usage
if __name__ == "__main__":
    # Example parameters
    start_time = "2025-06-02 00:00"
    total_hours = 24  # 24 hour s of forecast
    max_workers = 16  # Number of threads

    print(f"Starting weather data processing with {max_workers} threads...")
    dataset = main(start_time, total_hours, max_workers)

    if dataset is not None:
        print(f"\nProcessing completed successfully!")
        print(f"Dataset shape: {dataset.dims}")
        print(f"Variables: {list(dataset.data_vars.keys())}")
