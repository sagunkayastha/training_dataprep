import pyodbc
import pandas as pd

from herbie import Herbie


import numpy as np
from scipy.spatial import Delaunay
from sqlalchemy import create_engine
# Load environment variables from .env file


class HRRRDomainChecker:
    def __init__(self, config):
        """
        Initialize the HRRR domain checker with a dataset.

        Parameters:
        -----------
        ds : xarray.Dataset
            HRRR dataset containing 2D latitude and longitude arrays
        """
        self.config = config

    def get_sample(self):
        H = Herbie(
            "2021-07-19",
            model="hrrr",
            product="sfc",
            fxx=0,
            priority=["google", "aws"],
        )
        ds = H.xarray("TMP:2 m above", remove_grib=True)
        return ds

    def all_sites_coords(self, conn_str, table_name):
        """
        Get longitude and latitude for sites within North America boundaries

        Parameters:
        conn_str (str): Database connection string
        table_name (str): Name of the table to query

        Returns:
        pandas.DataFrame: DataFrame containing SiteId, SiteName, Latitude, and Longitude
        """
        try:
            # Connect to the database
            engine = create_engine(conn_str)
            # conn = pyodbc.connect(conn_str, timeout=30)

            # Query to get coordinates within North America
            query = f"""
            SELECT 
                Id,
                
                Latitude,
                Longitude
            FROM [{table_name}]
            
            """

            # Execute query and create DataFrame
            df = pd.read_sql(query, engine)

            return df

        except Exception as e:
            print(f"Error: {str(e)}")
            return None

    def is_point_in_domain(self, lat, lon):
        """
        Check if a point (latitude, longitude) is within the HRRR domain.

        Parameters:
        -----------
        lat : float
            Latitude in degrees
        lon : float
            Longitude in degrees (-180 to 180)

        Returns:
        --------
        bool
            True if point is within domain, False otherwise
        """
        point = np.array([lon, lat])
        return self.tri.find_simplex(point) >= 0

    def check_dataframe(self, df, lat_col="Latitude", lon_col="Longitude"):
        """
        Check multiple points from a DataFrame against the HRRR domain.

        Parameters:
        -----------
        df : pandas.DataFrame
            DataFrame containing latitude and longitude columns
        lat_col : str
            Name of the latitude column
        lon_col : str
            Name of the longitude column

        Returns:
        --------
        pandas.Series
            Series of 0s and 1s indicating if each point is within the domain (1) or not (0)
        """
        return df.apply(
            lambda row: int(self.is_point_in_domain(row[lat_col], row[lon_col])), axis=1
        )

    def get_domain_boundaries(self):
        """
        Get the approximate boundaries of the HRRR domain.

        Returns:
        --------
        tuple
            (lat_min, lat_max, lon_min, lon_max)
        """
        return (self.lat2d.min(), self.lat2d.max(), self.lon2d.min(), self.lon2d.max())

    def get_grid_info(self):
        """
        Get information about the HRRR grid.

        Returns:
        --------
        dict
            Dictionary containing grid information
        """
        return {
            "shape": self.lat2d.shape,
            "total_points": len(self.points),
            "lat_range": (self.lat2d.min(), self.lat2d.max()),
            "lon_range": (self.lon2d.min(), self.lon2d.max()),
        }

    def run(self, conn_str):
        # Create the checker once
        ds = self.get_sample()
        # Get the 2D lat/lon arrays
        self.lat2d = ds["latitude"].values
        self.lon2d = ds["longitude"].values

        # Convert lon to [-180, 180] if needed
        if self.lon2d.max() > 180:
            self.lon2d = np.where(self.lon2d > 180, self.lon2d - 360, self.lon2d)

        # Flatten the arrays and stack them into points
        self.points = np.column_stack((self.lon2d.flatten(), self.lat2d.flatten()))

        # Create a Delaunay triangulation of the grid points (do this once)
        self.tri = Delaunay(self.points)
        df = self.all_sites_coords(conn_str, "Site")

        results = self.check_dataframe(df)
        df["in_domain"] = results

        df = df[df["in_domain"] == 1]
        print(f"\nFound {len(df)} sites in HRRR Domain")
        return df


if __name__ == "__main__":
    # Example usage
    # You would need to load your HRRR dataset first
    # ds = xr.open_dataset('your_hrrr_file.nc')
    """
    server = os.getenv("DB_SERVER")
    username = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")
    database = os.getenv("DB_NAME")

    # Working connection string for Azure SQL Database
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "TrustServerCertificate=yes;"
        "Connection Timeout=30;"
        "Encrypt=yes;"
    )

    H = Herbie(
        "2021-07-19",
        model="hrrr",
        product="sfc",
        fxx=0,
    )
    ds = H.xarray("TMP:2 m above")

    # Create the checker once
    checker = HRRRDomainChecker(ds)
    df = all_sites_coords(conn_str, "Site")

    # Example of using with DataFrame
    # df = pd.DataFrame({
    #     'Latitude': [p[0] for p in test_points],
    #     'Longitude': [p[1] for p in test_points]
    # })

    results = checker.check_dataframe(df)
    df["in_domain"] = results

    df = df[df["in_domain"] == 1]
    print(f"\nFound {len(df)} sites in HRRR Domain")

    output_path = "files/sites_in_domain.csv"
    df.to_csv(output_path, index=False)
    """
    # print(results)

    # Example of getting domain information
    # boundaries = checker.get_domain_boundaries()
    # print(f"Domain boundaries: {boundaries}")
    # grid_info = checker.get_grid_info()
    # print(f"Grid information: {grid_info}")
