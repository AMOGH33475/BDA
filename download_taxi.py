import os
import urllib.request

# ============================================================
# CONFIGURATION
# ============================================================

OUTPUT_FOLDER = r"D:\COLLEGE\SEM 7\CNS\PROJECT\DATASET\NYC_Taxi_Data"

# 2015 through 2024
YEARS = range(2015, 2025)

# Target total downloaded size
TARGET_SIZE_GB = 4

# Official NYC TLC data server
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


# ============================================================
# CREATE OUTPUT DIRECTORY
# ============================================================

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

target_bytes = TARGET_SIZE_GB * 1024**3


# ============================================================
# CALCULATE SIZE OF ALREADY DOWNLOADED FILES
# ============================================================

def get_existing_size():

    size = 0

    for filename in os.listdir(OUTPUT_FOLDER):

        if filename.endswith(".parquet"):

            filepath = os.path.join(
                OUTPUT_FOLDER,
                filename
            )

            size += os.path.getsize(filepath)

    return size


total_size = get_existing_size()


# ============================================================
# DISPLAY STARTING INFORMATION
# ============================================================

print("=" * 70)
print("       NYC YELLOW TAXI DATASET DOWNLOADER")
print("=" * 70)

print(f"Target dataset size : {TARGET_SIZE_GB} GB")
print(f"Existing data       : {total_size / 1024**3:.2f} GB")
print(f"Output folder       : {OUTPUT_FOLDER}")

print("=" * 70)
print()


# ============================================================
# DOWNLOAD PROGRESS FUNCTION
# ============================================================

def download_progress(block_num, block_size, total_size):

    if total_size <= 0:
        return

    downloaded = block_num * block_size

    if downloaded > total_size:
        downloaded = total_size

    percent = downloaded / total_size * 100

    downloaded_mb = downloaded / 1024**2
    total_mb = total_size / 1024**2

    print(
        f"\rProgress: {percent:6.2f}% "
        f"({downloaded_mb:.2f} / {total_mb:.2f} MB)",
        end=""
    )


# ============================================================
# DOWNLOAD FILES
# ============================================================

for year in YEARS:

    for month in range(1, 13):

        # Stop once approximately 4 GB has been downloaded
        if total_size >= target_bytes:

            break


        # Example:
        # yellow_tripdata_2015-01.parquet

        filename = f"yellow_tripdata_{year}-{month:02d}.parquet"

        url = f"{BASE_URL}/{filename}"

        output_path = os.path.join(
            OUTPUT_FOLDER,
            filename
        )


        # ----------------------------------------------------
        # CHECK IF FILE ALREADY EXISTS
        # ----------------------------------------------------

        if os.path.exists(output_path):

            file_size = os.path.getsize(output_path)

            print(
                f"SKIPPING: {filename} "
                f"({file_size / 1024**2:.2f} MB already exists)"
            )

            continue


        # ----------------------------------------------------
        # START DOWNLOAD
        # ----------------------------------------------------

        print()
        print("-" * 70)
        print(f"Downloading: {filename}")
        print(f"URL: {url}")

        try:

            urllib.request.urlretrieve(
                url,
                output_path,
                reporthook=download_progress
            )

            print()


            # ------------------------------------------------
            # CHECK DOWNLOADED FILE
            # ------------------------------------------------

            if os.path.exists(output_path):

                file_size = os.path.getsize(output_path)

                total_size += file_size

                print(
                    f"Downloaded successfully: "
                    f"{file_size / 1024**2:.2f} MB"
                )

                print(
                    f"Total dataset size: "
                    f"{total_size / 1024**3:.2f} GB"
                )


        except Exception as error:

            print()
            print(f"ERROR downloading {filename}")
            print(f"Reason: {error}")


            # Delete incomplete file
            if os.path.exists(output_path):

                try:
                    os.remove(output_path)
                except:
                    pass


    # --------------------------------------------------------
    # CHECK TARGET AFTER EACH YEAR
    # --------------------------------------------------------

    if total_size >= target_bytes:

        break


# ============================================================
# FINAL RESULT
# ============================================================

print()
print()
print("=" * 70)
print("                 DOWNLOAD COMPLETE")
print("=" * 70)

print(
    f"Final dataset size : "
    f"{total_size / 1024**3:.2f} GB"
)

print(
    f"Dataset location   : "
    f"{OUTPUT_FOLDER}"
)

print("=" * 70)

print()
print("The dataset contains NYC Yellow Taxi trip records.")
print("The Parquet files can be read directly by Apache Spark.")
print()
print("Example Spark path:")
print(OUTPUT_FOLDER)