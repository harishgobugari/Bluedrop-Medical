import sys
import os
import logging
from etl_pipeline.etl import  extract, transform, load
from tests.test_etl import run_all_tests 


# Configure logging for the main script
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
   
    if len(sys.argv) < 2:
        logging.error("Usage: python3 transform_deliveries.py <path_to_json_file>")
        sys.exit(1)

    json_file_path = sys.argv[1]
    
    # Define output file name
    output_file_name = "output/tranformed_deliveries.csv" 

    logging.info(f"Starting ETL process for {json_file_path}")

    # Extract
    extracted_df = extract(json_file_path)
    if extracted_df.empty:
        logging.error("Extraction failed or returned empty data. Exiting.")
        sys.exit(1)

    # Transform
    transformed_df = transform(extracted_df)
    if transformed_df.empty:
        logging.error("Transformation failed or returned empty data. Exiting.")
        sys.exit(1)

    test_results = run_all_tests() 

    # Check if all tests passed
    if not test_results.wasSuccessful():
        logging.error("ETL process aborted due to failing unit tests.")
        sys.exit(1)
    logging.info("All unit tests passed successfully.")
    # --- End Unit Tests ---

    # Load
    load_successful = load(transformed_df, output_file_name)
    
    if not load_successful: 
        logging.error("ETL process aborted: Final data loading failed.")
        sys.exit(1)

    logging.info(f"ETL process completed. Final output saved to {output_file_name}")

if __name__ == "__main__":
    main()