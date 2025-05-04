"""
Simplified ETL runner without Great Expectations dependency.
"""
import os
import sys
import logging
from datetime import datetime

# Ensure the project root is in the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl.ingest import ingest

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_simple_etl(input_path=None, output_path=None):
    """
    Run a simplified ETL pipeline without Great Expectations.
    
    Args:
        input_path (str, optional): Path to input CSV file
        output_path (str, optional): Path to write Parquet output
    
    Returns:
        bool: True if the pipeline runs successfully
    """
    # Set default paths if not provided
    if input_path is None:
        input_path = "data/labels.csv"
    if output_path is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"data/output/parquet_{timestamp}"
    
    logger.info(f"Starting ETL pipeline at {datetime.now()}")
    logger.info(f"Input path: {input_path}")
    logger.info(f"Output path: {output_path}")
    
    try:
        # Run ETL
        logger.info("Starting ETL process")
        spark_session, output_path = ingest(input_path, output_path)
        
        # Validate by counting records
        logger.info("Validating output data")
        df = spark_session.read.parquet(output_path)
        row_count = df.count()
        logger.info(f"Output data contains {row_count} rows")
        
        # Check for null ImageID
        null_count = df.filter(df.ImageID.isNull()).count()
        logger.info(f"Output data contains {null_count} rows with null ImageID")
        
        if null_count > 0:
            logger.warning("Validation failed: Found rows with null ImageID")
        else:
            logger.info("Validation passed: No rows with null ImageID")
        
        # Clean up Spark session
        spark_session.stop()
        
        logger.info(f"ETL pipeline completed at {datetime.now()}")
        return True
    
    except Exception as e:
        logger.error(f"Error in ETL pipeline: {str(e)}")
        return False

if __name__ == "__main__":
    # When run directly, execute the ETL pipeline
    run_simple_etl()