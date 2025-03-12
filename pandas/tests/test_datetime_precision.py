import sys
sys.path.insert(0, "/workspaces/pandas")

import unittest
import pandas as pd
from io import BytesIO
from pandas.io.parquet import _fix_datetime_precision  # now importing from the local repo

class TestDatetimePrecision(unittest.TestCase):
    def test_datetime_precision(self):
        dates = pd.to_datetime(["2025-01-01 00:00:00", "2025-01-01 00:00:01"])
        dates = dates.astype("datetime64[s]")
        df_original = pd.DataFrame({"date": dates})
        
        print("Original dtype:", df_original["date"].dtype)  # Expected: datetime64[s]
        
        parquet_bytes = df_original.to_parquet()
        
        df_restored = pd.read_parquet(BytesIO(parquet_bytes), engine="pyarrow")
        
        df_restored = _fix_datetime_precision(df_restored)
        
        print("Restored dtype:", df_restored["date"].dtype)
        
        self.assertEqual(
            str(df_restored["date"].dtype),
            "datetime64[s]",
            f"Expected datetime64[s], but got {df_restored['date'].dtype}"
        )

if __name__ == "__main__":
    unittest.main()
