
import multiprocessing
import csv
import math
import os
import time

ZIP_PATH = "d:/tmp/zip_extracted/"
OUTPUT_PATH = 'combined_results.csv'
RADIUS_KM = 25
CENTER_LAT, CENTER_LON = 55.225000, 14.245000
MAX_ROWS = 20000000
PRINT_EVERY = 100000
MAX_ENCOUNTERS = 10
NUM_PROCESSES = os.cpu_count()

def equirectangular_approximation(lat1, lon1, lat2, lon2):
    """
    An approximation used for calculating the distances between different points.
    Expected to work reasonably well in smaller areas that can be assumed to be flat.
    """
    R = 6371.0  # radius of the Earth in kilometers
    x = math.radians(lon2 - lon1) * math.cos(0.5 * math.radians(lat1 + lat2))
    y = math.radians(lat2 - lat1)
    return R * math.sqrt(x*x + y*y)

def is_within_bounding_box(lat, lon):
    """
    Checks if the latitude and longitude are within a bounding box based on the center
    longitude and the radius of the circle. To be used for coarse grain filtering
    """
    min_lat = CENTER_LAT - RADIUS_KM/111
    max_lat = CENTER_LAT + RADIUS_KM/111
    min_lon = CENTER_LON - RADIUS_KM/111
    max_lon = CENTER_LON + RADIUS_KM/111
    return (min_lat <= lat <= max_lat) and (min_lon <= lon <= max_lon)

def limit_encounters(encounters, max_encounters):
    """
    If the length of the encounters dictionary exceeds the number of encounters allowed,
    sorts by shortest distance, and keeps only the top shortest distances.
    """
    if len(encounters) > max_encounters:
        sorted_encounters = sorted(encounters.items(), key=lambda x: x[1][0])
        encounters = dict(sorted_encounters[:max_encounters])
    return encounters

def process_file(file_name, output_file):
    latest_positions = {}
    encounters = {}

    with open(file_name, 'r') as file:
        reader = csv.DictReader(file)
        for i, row in enumerate(reader, 1):  
            if i > MAX_ROWS:
                break

            lat, lon = float(row['Latitude']), float(row['Longitude'])
            mmsi = row['MMSI']
            timestamp = row['# Timestamp']
            navigational_status = row['Navigational status']

            # Coarse grain filtering at first
            if (navigational_status == "Under way using engine") and is_within_bounding_box(lat, lon):
                current_distance = equirectangular_approximation(CENTER_LAT, CENTER_LON, lat, lon)
                
                # Finegrain filtering after
                if current_distance <= RADIUS_KM:
                    latest_positions[mmsi] = (timestamp, lat, lon)

                    # Record distances between vessel pairs:
                    for other_mmsi, (other_timestamp, other_lat, other_lon) in latest_positions.items():
                        if other_mmsi != mmsi:
                            distance = equirectangular_approximation(lat, lon, other_lat, other_lon)
                            
                            pair = tuple(sorted((mmsi, other_mmsi)))
                            if pair in encounters:
                                if distance < encounters[pair][0]:
                                    encounters[pair] = (distance, timestamp, other_timestamp, lat, lon, other_lat, other_lon)
                            else:
                                encounters[pair] = (distance, timestamp, other_timestamp, lat, lon, other_lat, other_lon)
                # Removing the vessel if it has moved out of the area
                elif mmsi in latest_positions:
                    del latest_positions[mmsi]  
                if i % PRINT_EVERY == 0:
                    print(f"Processed {i} rows")
                                        
                    encounters = limit_encounters(encounters, MAX_ENCOUNTERS)

    encounters = limit_encounters(encounters, MAX_ENCOUNTERS)
    with open(output_file, 'a', newline='') as f:
        writer = csv.writer(f)
        for key, values in encounters.items():
            row = list(key)+list(values)
            writer.writerow(row)

def main():
    output_file = OUTPUT_PATH
    file_names = [name for name in os.walk(ZIP_PATH)][0][2]
    file_paths = [ZIP_PATH+name for name in file_names]

    manager = multiprocessing.Manager()
    global lock
    lock = manager.Lock()

    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['mmsi1', 'mmsi2', 'distance', 'timestamp1', 'timestamp2', "latitude1", "longitude1", "latitude2", "longitude2"])

    # Parallelisation
    with multiprocessing.Pool(processes=NUM_PROCESSES) as pool:
        pool.starmap(process_file, [(fname, output_file) for fname in file_paths])
        pool.close()
        pool.join()

if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    elapsed_time = (end_time - start_time)/60
    print(f"Elapsed time: {elapsed_time:.2f} minutes")