"""
Simplified ETL runner using pandas instead of Spark for demo purposes.
"""
import os
import sys
import logging
import pandas as pd
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_pandas_etl(input_path=None, output_path=None):
    """
    Run a simplified ETL pipeline using pandas.
    
    Args:
        input_path (str, optional): Path to input CSV file
        output_path (str, optional): Path to write Parquet output
    
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
        output_path = f"data/output/parquet_{timestamp}"
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    logger.info(f"Starting ETL pipeline at {datetime.now()}")
    logger.info(f"Input path: {input_path}")
    logger.info(f"Output path: {output_path}")
    
    try:
        # Read input data
        logger.info("Reading input data")
        df = pd.read_csv(input_path)
        
        initial_count = len(df)
        logger.info(f"Initial row count: {initial_count}")
        
        # Transform: Filter out rows with null ImageID
        logger.info("Transforming data")
        df = df.dropna(subset=['ImageID'])
        
        transformed_count = len(df)
        logger.info(f"Row count after transformation: {transformed_count}")
        logger.info(f"Filtered out {initial_count - transformed_count} rows with null ImageID")
        
        # Write output data
        logger.info(f"Writing output data to {output_path}")
        df.to_parquet(output_path, index=False)
        
        # Basic validation
        logger.info("Validating output data")
        validation_df = pd.read_parquet(output_path)
        validation_count = len(validation_df)
        logger.info(f"Validated row count: {validation_count}")
        
        # Check that row counts match
        if validation_count != transformed_count:
            logger.warning(f"Row count mismatch: {transformed_count} vs {validation_count}")
        
        # Get a sample of rows for preview
        sample_rows = validation_df.head(5).to_dict(orient='records')
        
        logger.info(f"ETL pipeline completed at {datetime.now()}")
        
        return {
            "success": True,
            "timestamp": datetime.now().isoformat(),
            "statistics": {
                "initial_count": initial_count,
                "transformed_count": transformed_count,
                "filtered_count": initial_count - transformed_count,
                "validated_count": validation_count
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
    result = run_pandas_etl()
    if result["success"]:
        logger.info("ETL process completed successfully")
        logger.info(f"Statistics: {result['statistics']}")
    else:
        logger.error(f"ETL process failed: {result['error']}")