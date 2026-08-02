
# Runtime hook to handle connectorx metadata issues
import sys
import importlib.metadata

# Store original version function
original_version = importlib.metadata.version

# Create patched version function
def patched_version(distribution_name):
    try:
        return original_version(distribution_name)
    except (importlib.metadata.PackageNotFoundError, StopIteration):
        # Handle specific packages
        if distribution_name == 'connectorx':
            return '0.4.3'  # Hardcode the version
        # Let other package errors propagate normally
        raise

# Apply the patch
importlib.metadata.version = patched_version
print("Applied connectorx metadata patch")



import polars as pl

from polars_reverse_geocode import find_closest_city, find_closest_state, find_closest_country

df = pl.LazyFrame({
    'lat': [37.7749, 51.01, 52.5],
    'lon': [-122.4194, -3.9, -.91]
})

print(
    df.with_columns(
        city=find_closest_city('lat', 'lon'),
        state=find_closest_state('lat', 'lon'),
        country_code=find_closest_country('lat', 'lon')
    )
)


r = df.with_columns(
        city=find_closest_city('lat', 'lon'),
        state=find_closest_state('lat', 'lon'),
        country_code=find_closest_country('lat', 'lon')
    )
r.collect()