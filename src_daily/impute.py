from pathlib import Path
import pandas as pd
import numpy as np
from sklearn.experimental import enable_iterative_imputer  # noqa
from sklearn.impute import IterativeImputer
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from multiprocessing import Pool, cpu_count
import os
import glob
import json
import logging


class DataImputer:
    def __init__(
        self,
        config_path="config.json",
    ):
        """
        Initialize the DataImputer class.

        Args:
            config_path (str): Path to the configuration file
        """
        self.config = self._load_config(config_path)
        self.category = self.config["category_code"]

        self._setup_logging()
        self._setup_directories()

        self.training_data_dir = self.config["directories"]["data_root"]

        self.category_dir = os.path.join(self.training_data_dir, self.category)

        # Load configuration parameters
        self.met_vars = self.config["imputation"]["met_vars"]
        self.time_vars = self.config["imputation"]["time_vars"]
        self.rf_params = self.config["imputation"]["random_forest"]
        self.imputer_params = self.config["imputation"]["imputer"]
        self.cores = self.config["imputation"]["cores"]

    def _load_config(self, config_path):
        """Load configuration from JSON file."""
        try:
            with open(config_path, "r") as f:
                return json.load(f)
        except Exception as e:
            raise Exception(f"Error loading config file: {str(e)}")

    def _setup_logging(self):
        """Setup logging configuration."""
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(__name__)

    def _setup_directories(self):
        """Setup input and output directories."""
        # self.data_dir = Path("training_data")
        # self.category_dir = self.data_dir / self.config["category_code"]
        # self.output_dir = self.category_dir / "imputed_data"
        self.input_dir = (
            Path(self.config["directories"]["data_root"])
            / self.category
            / self.config["directories"]["site_data"]
            # / self.config["directories"]
        )

        self.output_dir = (
            Path(self.config["directories"]["data_root"])
            / self.category
            / self.config["directories"]["imputed_data"]
        )

        self.output_dir.mkdir(parents=True, exist_ok=True)

    def load_station(self, fn):
        """Load and prepare station data."""
        df = pd.read_csv(fn, parse_dates=["Date"]).sort_values("Date")
        return df.set_index("Date")

    def add_time_features(self, df):
        """Add time-based features to the dataframe."""
        df = df.copy()
        df["doy"] = df.index.dayofyear
        df["doy_sin"] = np.sin(2 * np.pi * df.doy / 365)
        df["doy_cos"] = np.cos(2 * np.pi * df.doy / 365)
        return df

    def impute_with_rf(self, df):
        """Perform imputation using Random Forest."""
        feats = self.met_vars + self.time_vars + ["PPM3"]
        X = df[feats].copy()

        # Scale meteorology + PPM3
        scaler = StandardScaler()
        X[self.met_vars + ["PPM3"]] = scaler.fit_transform(X[self.met_vars + ["PPM3"]])

        # Tree-based MICE
        imp = IterativeImputer(
            estimator=RandomForestRegressor(**self.rf_params), **self.imputer_params
        )
        X_filled = imp.fit_transform(X)

        # Un-scale PPM3
        ppm3_idx = feats.index("PPM3")
        ppm3_scaled = X_filled[:, ppm3_idx].reshape(-1, 1)
        dummy = np.zeros((len(ppm3_scaled), len(self.met_vars)))
        back = np.hstack([dummy, ppm3_scaled])
        ppm3_unscaled = scaler.inverse_transform(back)[:, -1]

        out = df.copy()
        out["PPM3_imputed"] = ppm3_unscaled
        return out

    def process_single_file(self, fn):
        """Process a single file - this function will be called in parallel."""
        try:
            # Convert string path to Path object if needed
            fn_path = Path(fn)

            # Load and process data
            df0 = self.load_station(fn_path)
            df1 = self.add_time_features(df0)
            df2 = self.impute_with_rf(df1)

            # Save output
            out_fn = self.output_dir / fn_path.name.replace(".csv", "_imputed.csv")
            df2.to_csv(out_fn)

            return f"✓ Processed {fn_path.name} -> {out_fn.name}"

        except Exception as e:
            return f"✗ Error processing {fn}: {str(e)}"

    def run(self):
        """Run the imputation process."""
        # Get all CSV files

        files = sorted(Path(self.input_dir).glob("*.csv"))
        # self.logger.info(f"Found {len(files)} files to process")

        # Get number of CPU cores
        n_cores = self.cores
        self.logger.info(f"Using {n_cores} CPU cores")

        # Process files in parallel
        with Pool(processes=n_cores) as pool:
            results = pool.map(self.process_single_file, [str(f) for f in files])

        # Print results
        # self.logger.info("\nProcessing Results:")
        for result in results:
            self.logger.info(result)

        self.logger.info(f"\nCompleted processing {len(files)} files")


if __name__ == "__main__":
    imputer = DataImputer()

    imputer.run()
