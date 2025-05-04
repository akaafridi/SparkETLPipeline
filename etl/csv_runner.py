"""
Simple CSV processing ETL runner without pandas/numpy dependencies.
"""
import os
import sys
import csv
import logging
import shutil
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_csv_etl(input_path=None, output_path=None):
    """
    Run a simple CSV ETL pipeline without pandas or numpy.
    
    Args:
        input_path (str, optional): Path to input CSV file
        output_path (str, optional): Path to write output CSV
    
    Returns:
        dict: Dictionary with ETL statistics and results
    """
    # Set default paths if not provided
    if input_path is None:
        input_path = "data/labels.csv"
    
    # Ensure input file exists
    if not os.path.exists(input_path):
        logger.error(f"Input file {input_path} does not exist")
        return {
            "success": False,
            "error": f"Input file {input_path} does not exist",
            "timestamp": datetime.now().isoformat()
        }
    
    if output_path is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = f"data/output/csv_{timestamp}"
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "processed_data.csv")
    else:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    logger.info(f"Starting ETL pipeline at {datetime.now()}")
    logger.info(f"Input path: {input_path}")
    logger.info(f"Output path: {output_path}")
    
    try:
        # Process the CSV file
        initial_count = 0
        transformed_count = 0
        filtered_count = 0
        sample_rows = []
        
        with open(input_path, 'r', newline='') as infile, open(output_path, 'w', newline='') as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)
            
            # Read the header
            header = next(reader)
            writer.writerow(header)
            
            # Find the index of the ImageID column
            try:
                imageid_index = header.index('ImageID')
            except ValueError:
                logger.error("CSV file does not contain an 'ImageID' column")
                return {
                    "success": False,
                    "error": "CSV file does not contain an 'ImageID' column",
                    "timestamp": datetime.now().isoformat()
                }
            
            # Process rows
            for row in reader:
                initial_count += 1
                
                # Skip rows with empty ImageID
                if imageid_index < len(row) and row[imageid_index].strip():
                    writer.writerow(row)
                    transformed_count += 1
                    
                    # Collect sample rows (up to 5)
                    if transformed_count <= 5:
                        sample_row = {}
                        for i, col in enumerate(header):
                            if i < len(row):
                                sample_row[col] = row[i]
                            else:
                                sample_row[col] = ""
                        sample_rows.append(sample_row)
                else:
                    filtered_count += 1
        
        # Create a copy in the data/output directory directly for easy reference
        fixed_output_path = "data/output/latest_processed.csv"
        shutil.copy(output_path, fixed_output_path)
        
        logger.info(f"ETL pipeline completed at {datetime.now()}")
        logger.info(f"Initial row count: {initial_count}")
        logger.info(f"Transformed row count: {transformed_count}")
        logger.info(f"Filtered row count: {filtered_count}")
        
        return {
            "success": True,
            "timestamp": datetime.now().isoformat(),
            "statistics": {
                "initial_count": initial_count,
                "transformed_count": transformed_count,
                "filtered_count": filtered_count,
            },
            "sample_rows": sample_rows,
            "output_path": output_path
        }
    
    except Exception as e:
        logger.error(f"Error in ETL pipeline: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

if __name__ == "__main__":
    # When run directly, execute the ETL pipeline
    result = run_csv_etl()
    if result["success"]:
        logger.info("ETL process completed successfully")
        logger.info(f"Statistics: {result['statistics']}")
    else:
        logger.error(f"ETL process failed: {result['error']}")