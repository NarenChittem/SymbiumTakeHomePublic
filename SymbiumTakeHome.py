import psycopg2
import json
import re
from esridump.dumper import EsriDumper
import time
import logging
import os

# Database connection details (user input)
DB_HOST = input("Enter the database host (default: 'localhost'): ") or 'localhost'
DB_PORT = input("Enter the database port (default: '5432'): ") or '5432'
DB_NAME = input("Enter the database name (default: 'el_dorado_county'): ") or 'el_dorado_county'
DB_USER = input("Enter the database username: ")
DB_PASSWORD = input("Enter the database password: ")

# Connect to the default database to create the new database if it doesn't exist
default_conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    user=DB_USER,
    password=DB_PASSWORD,
    database='postgres'  # Connect to the default 'postgres' database
)
default_conn.autocommit = True
default_cursor = default_conn.cursor()

# Check if the database exists, and create it if it doesn't
default_cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}'")
database_exists = default_cursor.fetchone()

if not database_exists:
    default_cursor.execute(f"CREATE DATABASE {DB_NAME}")
    print(f"Database '{DB_NAME}' created successfully.")
else:
    print(f"Database '{DB_NAME}' already exists.")

default_cursor.close()
default_conn.close()

DB_CONNECTION_STRING = f"dbname='{DB_NAME}' user='{DB_USER}' host='{DB_HOST}' password='{DB_PASSWORD}' port='{DB_PORT}'"

# ESRI layer URLs
PARCEL_LAYER_URL = "https://see-eldorado.edcgov.us/arcgis/rest/services/Symbium/SymbiumServices/MapServer/1"
ADDRESS_LAYER_URL = "https://see-eldorado.edcgov.us/arcgis/rest/services/Symbium/SymbiumServices/MapServer/0"

# Output file paths (user input)
OUTPUT_DIR = input("Enter the output directory (default: 'output'): ") or 'output'
PARCEL_GEOJSON_FILE = os.path.join(OUTPUT_DIR, "apns_geojson_using_pyesridump.geojson")
ADDRESS_GEOJSON_FILE = os.path.join(OUTPUT_DIR, "addresses_geojson_using_pyesridump.geojson")
CLEANED_ADDRESS_GEOJSON_FILE = os.path.join(OUTPUT_DIR, "cleaned_addresses_geojson1.geojson")
STANDARDIZED_PARCEL_GEOJSON_FILE = os.path.join(OUTPUT_DIR, "standardized_apns_geojson.geojson")

# Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_tables():
    commands = [
        "CREATE EXTENSION IF NOT EXISTS postgis;",
        """CREATE TABLE IF NOT EXISTS parcel (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            geom GEOMETRY(MultiPolygon, 4326),  -- Changed to MultiPolygon
            apn VARCHAR(255) UNIQUE
        );""",
        """CREATE TABLE IF NOT EXISTS parcel_apn (
            parcel_id UUID NOT NULL,
            apn VARCHAR(255) NOT NULL,
            CONSTRAINT fk_parcel
                FOREIGN KEY(parcel_id) 
                REFERENCES parcel(id)
                ON DELETE CASCADE
        );""",
        """CREATE TABLE IF NOT EXISTS parcel_address (
            id SERIAL PRIMARY KEY,
            parcel_id UUID NOT NULL,
            address TEXT NOT NULL,
            geom GEOMETRY(Point, 4326),
            CONSTRAINT fk_parcel_address
                FOREIGN KEY(parcel_id) 
                REFERENCES parcel(id)
                ON DELETE CASCADE
        );"""
    ]
    conn = None
    try:
        conn = psycopg2.connect(DB_CONNECTION_STRING)
        cur = conn.cursor()
        for command in commands:
            cur.execute(command)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def download_and_save_layer_as_geojson(url, output_filename, max_retries=3):
    attempt = 0
    while attempt < max_retries:
        try:
            dumper = EsriDumper(url, timeout=1500)
            features = []
            for feature in dumper:
                features.append(feature)
            
            # Save the features to a GeoJSON file
            with open(output_filename, 'w') as f:
                json.dump({
                    "type": "FeatureCollection",
                    "features": features
                }, f, indent=4)
            
            logger.info(f"Downloaded and saved {len(features)} features to {output_filename}")
            return  # Successful download, exit the function
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed with error: {e}")
            attempt += 1
            time.sleep(5)  # Wait for 5 seconds before retrying
    
    logger.error(f"Failed to download data after {max_retries} attempts.")

def clean_apn(apn):
    """Remove non-alphanumeric characters from APN."""
    return re.sub(r'\W+', '', str(apn))

def standardize_address_component(component):
    """Convert to string, trim spaces, and capitalize address components."""
    if not isinstance(component, str):
        component = str(component)  # Convert to string if not already
    return ' '.join(filter(None, component.split())).title()

def is_address_complete(properties):
    """Check if essential address components are present and non-empty."""
    essential_components = ['ADDR_NBR', 'NAME_ROOT']  # Define essential components
    for component in essential_components:
        if component not in properties or not str(properties[component]).strip():
            return False  # Incomplete if any essential component is missing or empty
    return True  # Complete if all essential components are present and non-empty

def unique_address_key(properties, geometry):
    """Generate a unique key for the address based on its components and geometry."""
    address_components = [
        str(properties.get(field, '')).strip().title() for field in [
            'ADDR_NBR', 'ADDR_STR_NBR', 'PREFIX', 'NAME_ROOT', 'SUFFIX', 'ADDR_UNIT_NBR'
        ]
    ]
    full_address = ' '.join(filter(None, address_components)).upper()
    geo_data = str(geometry['coordinates']) if geometry.get('coordinates') else 'NO_COORDINATES'
    return f"{full_address}_{geo_data}"

def clean_address_dataset(geojson_file_path, output_file_path):
    with open(geojson_file_path) as f:
        data = json.load(f)

    cleaned_features = []
    seen_addresses = set()

    for feature in data['features']:
        properties = feature['properties']
        geometry = feature['geometry']
        
        if not properties.get('PRCL_ID', '').strip():
            continue

        if not is_address_complete(properties):
            continue
        
        properties['PRCL_ID'] = clean_apn(properties['PRCL_ID'])

        for component in ['ADDR_NBR', 'PREFIX', 'NAME_ROOT', 'SUFFIX', 'ADDR_UNIT_TYPE', 'ADDR_UNIT_NBR', 'ADDR_FLOOR']:
            if component in properties:
                properties[component] = standardize_address_component(properties.get(component, ''))

        address_key = unique_address_key(properties, geometry)
        if address_key not in seen_addresses:
            seen_addresses.add(address_key)
            cleaned_features.append(feature)

    data['features'] = cleaned_features
    
    with open(output_file_path, 'w') as f:
        json.dump(data, f, indent=4)

def clean_apn_dataset(geojson_file_path, output_file_path):
    with open(geojson_file_path) as f:
        data = json.load(f)

    cleaned_features = []

    for feature in data['features']:
        properties = feature['properties']
        
        # Clean and standardize APN
        if 'PRCL_ID' in properties:
            properties['PRCL_ID'] = clean_apn(properties['PRCL_ID'])
        
        cleaned_features.append(feature)

    # Update the data with cleaned features
    data['features'] = cleaned_features
    
    # Save the cleaned data to a new GeoJSON file
    with open(output_file_path, 'w') as f:
        json.dump(data, f, indent=4)

def verify_cleaned_data(geojson_file_path):
    with open(geojson_file_path) as f:
        data = json.load(f)

    total_features = 0
    issues_found = 0
    address_counter = {}  # Track occurrences of addresses with their geo data

    for feature in data['features']:
        properties = feature['properties']
        geometry = feature['geometry']
        total_features += 1
        
        # Construct the full address from components, ensuring all parts are treated as strings
        address_components = [
            str(properties.get(field, '')).strip().title() for field in [
                'ADDR_NBR', 'ADDR_STR_NBR', 'PREFIX', 'NAME_ROOT', 'SUFFIX', 'ADDR_UNIT_NBR'
            ]
        ]
        # Filter out empty components and join to form a full address
        full_address = ' '.join(filter(None, address_components)).upper()

        # Include the geo coordinates as part of the unique identifier for each address
        # Assuming the coordinates are a list of [longitude, latitude]
        geo_data = str(geometry['coordinates']) if geometry['coordinates'] else 'NO_COORDINATES'

        # Use both address and geo_data as key
        unique_key = f"{full_address}_{geo_data}"

        if not full_address:
            print(f"Issue found in feature {total_features}: Incomplete address")
            issues_found += 1
        else:
            address_counter[unique_key] = address_counter.get(unique_key, 0) + 1

    # Check for duplicate addresses (considering geo data)
    duplicate_addresses = {key.split('_')[0]: count for key, count in address_counter.items() if count > 1}
    if duplicate_addresses:
        print("Duplicate addresses with identical geo data found:")
        for address, count in duplicate_addresses.items():
            print(f"Address '{address}' occurs {count} times")
        issues_found += len(duplicate_addresses)

    print(f"Verification completed. Total features checked: {total_features}, Issues found: {issues_found}")

def verify_cleaned_apns(geojson_file_path):
    with open(geojson_file_path) as f:
        data = json.load(f)

    total_features = 0
    issues_found = 0

    for feature in data['features']:
        total_features += 1
        properties = feature['properties']
        
        apn = properties.get('PRCL_ID', '')
        # Verify that APN is standardized (only alphanumeric characters)
        if not re.match(r'^[a-zA-Z0-9]*$', apn):
            print(f"Issue found in feature {total_features}: Non-standard APN '{apn}'")
            issues_found += 1

    print(f"Verification completed. Total features checked: {total_features}, Issues found: {issues_found}")

def upload_for_parcel(geojson_file_path, db_connection_string):
    with open(geojson_file_path, 'r') as file:
        data = json.load(file)
    
    conn = psycopg2.connect(db_connection_string)
    cursor = conn.cursor()
    
    insert_query = """
    INSERT INTO parcel (geom, apn) 
    VALUES (ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326), %s)
    ON CONFLICT (apn) DO NOTHING;
    """
    
    for feature in data['features']:
        geom = json.dumps(feature['geometry'])
        apn = feature['properties']['PRCL_ID']
        cursor.execute(insert_query, (geom, apn))
    
    conn.commit()
    cursor.close()
    conn.close()
    print("Uploaded parcel data to the database.")

def correct_or_drop_invalid_geometries(db_connection_string):
    conn = psycopg2.connect(db_connection_string)
    cursor = conn.cursor()
    
    # Identify parcels with invalid geometries
    cursor.execute("SELECT id FROM parcel WHERE NOT ST_IsValid(geom);")
    invalid_parcel_ids = cursor.fetchall()

    for parcel_id_tuple in invalid_parcel_ids:
        parcel_id = parcel_id_tuple[0]
        # Try to correct invalid geometries to MultiPolygon
        cursor.execute("""
            WITH corrected AS (
                SELECT ST_CollectionExtract(ST_MakeValid(geom), 3) AS geom
                FROM parcel WHERE id = %s
            ), multi AS (
                SELECT ST_Multi(geom) AS geom FROM corrected WHERE geom IS NOT NULL
            )
            UPDATE parcel
            SET geom = (SELECT geom FROM multi)
            WHERE id = %s AND (SELECT geom FROM multi) IS NOT NULL;
        """, (parcel_id, parcel_id))

        # If geometry cannot be corrected to MultiPolygon, drop the parcel
        cursor.execute("""
            DELETE FROM parcel
            WHERE id = %s AND (NOT ST_IsValid(geom) OR geom IS NULL);
        """, (parcel_id,))
        
        conn.commit()

    cursor.close()
    conn.close()
    print("Finished processing invalid geometries.")

def check_geometry_issues(db_connection_string):
    conn = psycopg2.connect(db_connection_string)
    cursor = conn.cursor()
    
    # Check for any remaining invalid geometries
    cursor.execute("SELECT COUNT(*) FROM parcel WHERE NOT ST_IsValid(geom);")
    invalid_geom_count = cursor.fetchone()[0]
    if invalid_geom_count == 0:
        print("No invalid geometries found.")
    else:
        print(f"Found {invalid_geom_count} invalid geometries.")
    
    # Check for any GeometryCollection types
    cursor.execute("SELECT COUNT(*) FROM parcel WHERE GeometryType(geom) = 'GEOMETRYCOLLECTION';")
    geometry_collection_count = cursor.fetchone()[0]
    if geometry_collection_count == 0:
        print("No GeometryCollection types found.")
    else:
        print(f"Found {geometry_collection_count} GeometryCollection types.")
    
    cursor.close()
    conn.close()

def upload_for_parcel_apn(db_connection_string):
    conn = psycopg2.connect(db_connection_string)
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO parcel_apn (parcel_id, apn)
        SELECT id, apn FROM parcel;
    """)

    conn.commit()
    cursor.close()
    conn.close()

    print("Parcel APN table populated successfully.")

def upload_for_parcel_address(db_connection_string, addresses_geojson_path):
    with open(addresses_geojson_path, 'r') as file:
        addresses_data = json.load(file)

    conn = psycopg2.connect(db_connection_string)
    cursor = conn.cursor()

    for feature in addresses_data['features']:
        address_components = [standardize_address_component(feature['properties'].get(field)) for field in ['ADDR_NBR', 'ADDR_STR_NBR', 'PREFIX', 'NAME_ROOT', 'SUFFIX', 'ADDR_UNIT_NBR']]
        address = ' '.join(filter(None, address_components)).strip()
        prcl_id = feature['properties'].get('PRCL_ID')
        geom_json = json.dumps(feature['geometry'])

        if not prcl_id or not address:
            continue

        # Pre-insertion check for duplicate addresses
        cursor.execute("""
            SELECT EXISTS(
                SELECT 1
                FROM parcel_address
                WHERE address = %s
            );
        """, (address,))
        exists = cursor.fetchone()[0]

        if exists:
            continue

        cursor.execute("""
            INSERT INTO parcel_address (parcel_id, address, geom)
            SELECT papn.parcel_id, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)
            FROM parcel_apn papn
            WHERE papn.apn = %s
            ON CONFLICT DO NOTHING;
        """, (address, geom_json, prcl_id))

    conn.commit()
    cursor.close()
    conn.close()

    print("Addresses uploaded and associated with parcels by APN.")

def test_parcel_upload(db_connection_string):
    conn = psycopg2.connect(db_connection_string)
    cursor = conn.cursor()

    try:
        # Check if the parcel table exists
        cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'parcel');")
        table_exists = cursor.fetchone()[0]
        assert table_exists, "Parcel table does not exist"

        # Check if the parcel table has records
        cursor.execute("SELECT COUNT(*) FROM parcel;")
        record_count = cursor.fetchone()[0]
        assert record_count > 0, "No records found in the parcel table"

        # Check if the geometries are valid
        cursor.execute("SELECT COUNT(*) FROM parcel WHERE ST_IsValid(geom);")
        valid_geom_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM parcel;")
        total_count = cursor.fetchone()[0]
        assert valid_geom_count == total_count, "Invalid geometries found in the parcel table"

        print("Parcel upload test passed!")

    except AssertionError as e:
        print(f"Test failed: {str(e)}")

    finally:
        cursor.close()
        conn.close()

def test_parcel_apn_relationships(db_connection_string):
    conn = psycopg2.connect(db_connection_string)
    cursor = conn.cursor()

    try:
        # Check if the parcel_apn table exists
        cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'parcel_apn');")
        table_exists = cursor.fetchone()[0]
        assert table_exists, "parcel_apn table does not exist"

        # Check if the parcel_apn table has records
        cursor.execute("SELECT COUNT(*) FROM parcel_apn;")
        record_count = cursor.fetchone()[0]
        assert record_count > 0, "No records found in the parcel_apn table"

        # Check if all parcel_id values in parcel_apn exist in the parcel table
        cursor.execute("""
            SELECT COUNT(*)
            FROM parcel_apn pa
            LEFT JOIN parcel p ON pa.parcel_id = p.id
            WHERE p.id IS NULL;
        """)
        missing_parcel_count = cursor.fetchone()[0]
        assert missing_parcel_count == 0, "parcel_apn table contains parcel_id values that do not exist in the parcel table"

        # Check if all APNs in parcel_apn match the corresponding APNs in the parcel table
        cursor.execute("""
            SELECT COUNT(*)
            FROM parcel_apn pa
            JOIN parcel p ON pa.parcel_id = p.id
            WHERE pa.apn <> p.apn;
        """)
        mismatched_apn_count = cursor.fetchone()[0]
        assert mismatched_apn_count == 0, "APNs in parcel_apn table do not match the corresponding APNs in the parcel table"

        print("Parcel APN relationship test passed!")

    except AssertionError as e:
        print(f"Test failed: {str(e)}")

    finally:
        cursor.close()
        conn.close()

def test_address_upload(db_connection_string):
    conn = psycopg2.connect(db_connection_string)
    cursor = conn.cursor()

    try:
        # Check if the parcel_address table exists
        cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'parcel_address');")
        table_exists = cursor.fetchone()[0]
        assert table_exists, "parcel_address table does not exist"

        # Check if the parcel_address table has records
        cursor.execute("SELECT COUNT(*) FROM parcel_address;")
        record_count = cursor.fetchone()[0]
        assert record_count > 0, "No records found in the parcel_address table"

        # Check if all parcel_id values in parcel_address exist in the parcel table
        cursor.execute("""
            SELECT COUNT(*)
            FROM parcel_address pa
            LEFT JOIN parcel p ON pa.parcel_id = p.id
            WHERE p.id IS NULL;
        """)
        missing_parcel_count = cursor.fetchone()[0]
        assert missing_parcel_count == 0, "parcel_address table contains parcel_id values that do not exist in the parcel table"

        # Check if all parcel_id values in parcel_address are associated with valid APNs in the parcel_apn table
        cursor.execute("""
            SELECT COUNT(*)
            FROM parcel_address pa
            LEFT JOIN parcel_apn papn ON pa.parcel_id = papn.parcel_id
            WHERE papn.parcel_id IS NULL;
        """)
        missing_apn_count = cursor.fetchone()[0]
        assert missing_apn_count == 0, "parcel_address table contains parcel_id values that do not have associated APNs in the parcel_apn table"

        # Check for addresses associated with multiple parcels
        cursor.execute("""
            SELECT address, COUNT(DISTINCT parcel_id) AS parcel_count
            FROM parcel_address
            GROUP BY address
            HAVING COUNT(DISTINCT parcel_id) > 1;
        """)
        multi_parcel_addresses = cursor.fetchall()

        if len(multi_parcel_addresses) > 0:
            print("Addresses associated with multiple parcels:")
            for address, parcel_count in multi_parcel_addresses:
                print(f"Address: {address}, Parcel Count: {parcel_count}")
            raise AssertionError("Addresses found associated with multiple parcels")

        # Check if there are any duplicate addresses within the same parcel
        cursor.execute("""
            SELECT COUNT(*)
            FROM (
                SELECT address, parcel_id, COUNT(*)
                FROM parcel_address
                GROUP BY address, parcel_id
                HAVING COUNT(*) > 1
            ) AS duplicates;
        """)
        duplicate_count = cursor.fetchone()[0]
        assert duplicate_count == 0, "Duplicate addresses found within the same parcel"

        print("Address upload test passed!")

    except AssertionError as e:
        print(f"Test failed: {str(e)}")

    finally:
        cursor.close()
        conn.close()

def run_general_tests(db_connection_string):
    conn = psycopg2.connect(db_connection_string)
    cursor = conn.cursor()

    # Test APN format (alphanumeric) and uniqueness
    cursor.execute("SELECT COUNT(*) FROM parcel WHERE NOT apn ~* '^[a-z0-9]+$';")
    invalid_apn_format_count = cursor.fetchone()[0]
    assert invalid_apn_format_count == 0, "APNs with invalid format found"

    cursor.execute("SELECT apn, COUNT(*) FROM parcel GROUP BY apn HAVING COUNT(*) > 1;")
    duplicate_apns = cursor.fetchall()
    assert len(duplicate_apns) == 0, "Duplicate APNs found"

    # Test for completeness of the address field
    cursor.execute("SELECT COUNT(*) FROM parcel_address WHERE trim(address) = '';")
    incomplete_address_count = cursor.fetchone()[0]
    assert incomplete_address_count == 0, "Incomplete addresses found"

    # Test that address point geometries lie within a 0.5 meter buffer of their associated parcel geometries
    cursor.execute("""
        SELECT COUNT(pa.*)
        FROM parcel_address pa
        JOIN parcel p ON pa.parcel_id = p.id
        WHERE NOT ST_Within(pa.geom, ST_Buffer(p.geom, 0.5)); 
    """)
    outside_geom_count = cursor.fetchone()[0]
    assert outside_geom_count == 0, "Address geometries found outside of their associated parcels (within a 0.5 meter buffer)"

    # Test for addresses without a parcel
    cursor.execute("SELECT COUNT(*) FROM parcel_address WHERE parcel_id NOT IN (SELECT id FROM parcel);")
    orphan_addresses_count = cursor.fetchone()[0]
    assert orphan_addresses_count == 0, "Orphan addresses found without an associated parcel"

    cursor.close()
    conn.close()
    print("General tests passed!")

def database_integrity_check(db_connection_string):
    conn = psycopg2.connect(db_connection_string)
    cursor = conn.cursor()

    # Verify parcel table integrity
    cursor.execute("SELECT COUNT(*), COUNT(DISTINCT apn) FROM parcel;")
    total, unique_apns = cursor.fetchone()
    print(f"Parcel Table: {total} records, {unique_apns} unique APNs.")

    # Verify address table integrity
    cursor.execute("SELECT COUNT(*), COUNT(DISTINCT address) FROM parcel_address;")
    total, unique_addresses = cursor.fetchone()
    print(f"Address Table: {total} records, {unique_addresses} unique addresses.")

    # Spatial relationship check (sample)
    cursor.execute("""
        SELECT COUNT(*) FROM parcel_address pa
        JOIN parcel p ON pa.parcel_id = p.id
        WHERE ST_Contains(p.geom, pa.geom);
    """)
    valid_address_locations = cursor.fetchone()[0]
    print(f"Addresses correctly located within parcels: {valid_address_locations}")

    cursor.close()
    conn.close()

def run_tests(db_connection_string):
    test_parcel_upload(db_connection_string)
    test_parcel_apn_relationships(db_connection_string)
    test_address_upload(db_connection_string)
    run_general_tests(db_connection_string)

def main():
    # Create the output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Create the necessary tables
    create_tables()

    # Download and save ESRI layers as GeoJSON files
    download_and_save_layer_as_geojson(PARCEL_LAYER_URL, PARCEL_GEOJSON_FILE)
    download_and_save_layer_as_geojson(ADDRESS_LAYER_URL, ADDRESS_GEOJSON_FILE)

    # Clean and standardize the parcel and address datasets
    clean_address_dataset(ADDRESS_GEOJSON_FILE, CLEANED_ADDRESS_GEOJSON_FILE)
    clean_apn_dataset(PARCEL_GEOJSON_FILE, STANDARDIZED_PARCEL_GEOJSON_FILE)

    # Verify the cleaned datasets
    verify_cleaned_data(CLEANED_ADDRESS_GEOJSON_FILE)
    verify_cleaned_apns(STANDARDIZED_PARCEL_GEOJSON_FILE)

    # Upload the parcel data to the database
    upload_for_parcel(STANDARDIZED_PARCEL_GEOJSON_FILE, DB_CONNECTION_STRING)

    # Correct or drop invalid geometries in the parcel table
    correct_or_drop_invalid_geometries(DB_CONNECTION_STRING)

    # Check for geometry issues in the parcel table
    check_geometry_issues(DB_CONNECTION_STRING)

    # Populate the parcel_apn table
    upload_for_parcel_apn(DB_CONNECTION_STRING)

    # Upload the address data and associate with parcels by APN
    upload_for_parcel_address(DB_CONNECTION_STRING, CLEANED_ADDRESS_GEOJSON_FILE)

    # Run tests
    run_tests(DB_CONNECTION_STRING)

if __name__ == "__main__":
    main()