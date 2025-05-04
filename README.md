# ETL Demo

This project demonstrates an end-to-end ETL (Extract, Transform, Load) pipeline with a Flask web interface for monitoring and execution.

![ETL Demo](https://via.placeholder.com/800x400?text=ETL+Demo+Screenshot)

## Prerequisites & Setup

- **Python Version**: Python 3.11+ 
- **Dependencies**: Flask and its dependencies
- **No Virtual Environment Required**: The project runs natively in Replit

### Installation

```bash
# Install required packages
pip install flask
```

## Installation & Run Instructions

To start the Flask application:

```bash
# Start the application
python main.py
```

The application will be available at:
- URL: http://localhost:5000 (local development)
- URL: The Replit URL (when running on Replit)

You can override the default port by modifying the port value in `main.py`:
```python
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)  # Change 5000 to your desired port
```

## Usage Examples

### Running the ETL Pipeline

1. Open the web interface
2. Click the "Run ETL Pipeline" button
3. View the results displayed in the interface

### Sample Output

```csv
ImageID,LabelName,Confidence
000a1249af2bc5f0,/m/0242l,1
0011eb6f9f2fd06f,/m/0138tl,1
00156515f5d9c6c4,/m/02p5f1q,1
...
```

### API Response

```json
{
  "success": true,
  "timestamp": "2025-05-04T12:45:50.782933",
  "statistics": {
    "initial_count": 50,
    "transformed_count": 47,
    "filtered_count": 3
  },
  "sample_rows": [
    {
      "ImageID": "000a1249af2bc5f0",
      "LabelName": "/m/0242l",
      "Confidence": "1"
    },
    ...
  ],
  "output_path": "data/output/csv_20250504_124550/processed_data.csv"
}
```

## Project Structure Commentary

- `data/`: Contains input and output data files
  - `labels.csv`: Source data file with image labels
  - `output/`: Generated output files with timestamps (created during ETL)
- `etl/`: Core ETL logic modules
  - `csv_runner.py`: **Main ETL implementation** - Simple CSV processor without dependencies
  - `ingest.py`: Reference Spark implementation (currently not used due to compatibility)
  - `expectations/`: Great Expectations configuration (for future validation)
  - `run.py`: Alternative runner with Great Expectations integration (reference)
  - `dag.py`: Prefect workflow definition for orchestration (reference)
- `main.py`: Flask web application with routes and UI

## Configuration & Extensibility

### Customizing Input/Output Paths

Edit the `run_csv_etl` function in `etl/csv_runner.py` to change default paths:

```python
# Modify these values to change input/output locations
if input_path is None:
    input_path = "data/labels.csv"
    
if output_path is None:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"data/output/csv_{timestamp}"
```

### Extending the ETL Process

To add new transformations, modify the processing loop in `etl/csv_runner.py`:

```python
# Process rows
for row in reader:
    initial_count += 1
    
    # Add your custom transformations here
    
    # Skip rows with empty ImageID
    if imageid_index < len(row) and row[imageid_index].strip():
        writer.writerow(row)
        transformed_count += 1
        ...
```

## Features

- CSV data processing and transformation
- Data validation and filtering
- Interactive web interface with detailed results
- AJAX-powered ETL execution with real-time feedback

## ETL Process

The ETL pipeline consists of the following steps:

1. **Extract**: Read CSV data from `data/labels.csv`
2. **Transform**: Filter out records with null ImageID values
3. **Load**: Write the processed data to CSV format in `data/output/csv_<timestamp>`
4. **Validate**: Basic validation to check for null values and row counts

## Results Displayed

After running the pipeline, the interface displays:
- Initial record count
- Processed record count
- Filtered record count (records with null values)
- Timestamp of execution
- Sample data from the output file

## Future Work / Roadmap

### Planned Enhancements

- **Spark Integration**: Re-introduce Apache Spark for large-scale data processing once compatibility issues are resolved
- **Great Expectations**: Add comprehensive data validation with Great Expectations
- **Automated Testing**: Implement unit and integration tests with pytest
- **Scheduling**: Add job scheduling via cron or a dedicated scheduler
- **Docker Support**: Containerize the application for consistent deployment

### Technical Debt

- Resolve compatibility issues between numpy, pandas, and other dependencies
- Improve error handling and logging
- Add proper configuration management system

## Running the ETL Pipeline Directly

You can also run the ETL pipeline directly from the command line:
```bash
python -m etl.csv_runner
```

## Contribution & Licensing

### Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### License

This project is licensed under the MIT License - see the LICENSE file for details.

## Connecting to GitHub

You can connect this project to GitHub by following these steps:

1. In Replit, click on the Version Control tab in the left sidebar
2. Click "Connect to GitHub"
3. Authorize Replit to access your GitHub account
4. Create a new repository or select an existing one
5. Push your changes