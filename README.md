# SymbiumTakeHome

# README

This Python script downloads, cleans, and processes GIS data about properties, APNs (Assessor's Parcel Numbers), and addresses in unincorporated El Dorado County. The processed data is then uploaded to a PostgreSQL database for use by Symbium.

## Assumptions

1. **0.5 meter buffer for address point geometries:** In the `run_general_tests` function, a test is performed to check if address point geometries lie within a 0.5 meter buffer of their associated parcel geometries. This assumption is made to account for potential errors in the geospatial data. If an address point is outside its associated parcel but within 0.5 meters of it, it is considered correct. This is a fair assumption given the small distance, which likely indicates a minor discrepancy in the data rather than a significant issue.

2. **Invalid addresses are filtered out:** Addresses with missing or empty street names are considered invalid and are filtered out during the cleaning process.

3. **Null geometries are filtered out:** The script filters out any null geometries from the dataset.

4. **Broken geometries are fixed:** The script attempts to fix any broken geometries using the `ST_MakeValid` function in PostGIS. If a geometry cannot be fixed, it is dropped from the dataset.

## Dependencies
- Python 3.x
- PostgreSQL
- PostGIS
- `psycopg2` library for connecting to PostgreSQL
- `esridump` library for downloading ESRI layers
- `gdal` library for importing GeoJSON data into PostgreSQL
- PostgreSQL Extensions:
  - `postgis` for processing geometries and geometry-related joins
  - `uuid-ossp` for assigning identifiers to parcels
## Instructions

1. Install the required dependencies mentioned above.

2. Update the database connection details in the script:
   - `DB_HOST`: Database host (default: 'localhost')
   - `DB_PORT`: Database port (default: '5432')
   - `DB_NAME`: Database name (default: 'el_dorado_county')
   - `DB_USER`: Database username
   - `DB_PASSWORD`: Database password

3. Run the script using the command: `SymbiumTakeHome.py`

4. The script will download the ESRI layers, clean and process the data, and upload it to the specified PostgreSQL database.

5. The processed data will be stored in the following tables:
   - `parcel`: Contains parcel identifiers and parcel geometries.
   - `parcel_apn`: Contains parcel identifiers and associated APNs.
   - `parcel_address`: Contains parcel identifiers and associated addresses.

6. The script also includes tests to validate the processed data. The tests check for APN format and uniqueness, address completeness, address point geometries within parcel boundaries, and orphan addresses without associated parcels.

## Known Bugs

- None identified at the moment.

Please let me know if you have any further questions or if there are any issues with running the script.
