from flask import Flask, render_template_string, redirect, url_for, jsonify
import os
import logging
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create Flask app
app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "dev-secret-key")

# Enhanced HTML template with results display
html_template = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ETL Demo</title>
    <link rel="stylesheet" href="https://cdn.replit.com/agent/bootstrap-agent-dark-theme.min.css">
    <style>
        .results-container {
            display: none;
        }
        #loading {
            display: none;
        }
    </style>
</head>
<body>
    <div class="container mt-5">
        <div class="row">
            <div class="col-md-10 offset-md-1">
                <div class="card mb-4">
                    <div class="card-header bg-dark text-white">
                        <h2>ETL Demo</h2>
                    </div>
                    <div class="card-body">
                        <h4>ETL Pipeline Status</h4>
                        <div class="alert {{ alert_class }}" role="alert">
                            {{ status_message }}
                        </div>
                        
                        <h4 class="mt-4">Project Overview</h4>
                        <p>This demo shows an end-to-end ETL pipeline that:</p>
                        <ol>
                            <li>Reads CSV data from <code>data/labels.csv</code></li>
                            <li>Filters out records with null ImageID values</li>
                            <li>Writes the processed data to CSV format</li>
                            <li>Validates the output data</li>
                        </ol>
                        
                        <h4 class="mt-4">Actions</h4>
                        <button id="run-etl-btn" class="btn btn-primary mb-3">Run ETL Pipeline</button>
                        <div id="loading" class="d-flex align-items-center mt-2">
                            <div class="spinner-border text-primary me-2" role="status">
                                <span class="visually-hidden">Loading...</span>
                            </div>
                            <span>Processing data...</span>
                        </div>
                    </div>
                </div>
                
                <!-- Results Section -->
                <div id="results-container" class="card mb-4 results-container">
                    <div class="card-header bg-dark text-white">
                        <h4 class="m-0">ETL Results</h4>
                    </div>
                    <div class="card-body">
                        <div class="row">
                            <div class="col-md-6">
                                <div class="card mb-3">
                                    <div class="card-header">Statistics</div>
                                    <div class="card-body">
                                        <table class="table table-sm">
                                            <tbody>
                                                <tr>
                                                    <th>Initial Records</th>
                                                    <td id="initial-count">-</td>
                                                </tr>
                                                <tr>
                                                    <th>Processed Records</th>
                                                    <td id="processed-count">-</td>
                                                </tr>
                                                <tr>
                                                    <th>Filtered Records</th>
                                                    <td id="filtered-count">-</td>
                                                </tr>
                                                <tr>
                                                    <th>Output File</th>
                                                    <td id="output-path">-</td>
                                                </tr>
                                                <tr>
                                                    <th>Timestamp</th>
                                                    <td id="timestamp">-</td>
                                                </tr>
                                            </tbody>
                                        </table>
                                    </div>
                                </div>
                            </div>
                            <div class="col-md-6">
                                <div class="card">
                                    <div class="card-header">Sample Data</div>
                                    <div class="card-body">
                                        <div class="table-responsive">
                                            <table id="sample-data" class="table table-sm">
                                                <thead id="sample-header">
                                                </thead>
                                                <tbody id="sample-body">
                                                </tbody>
                                            </table>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                
                <div class="card">
                    <div class="card-header bg-dark text-white">
                        <h4 class="m-0">Project Structure</h4>
                    </div>
                    <div class="card-body">
                        <ul class="list-group">
                            <li class="list-group-item d-flex justify-content-between align-items-start">
                                <div class="ms-2 me-auto">
                                    <div class="fw-bold"><code>data/labels.csv</code></div>
                                    Input data file
                                </div>
                            </li>
                            <li class="list-group-item d-flex justify-content-between align-items-start">
                                <div class="ms-2 me-auto">
                                    <div class="fw-bold"><code>data/output/</code></div>
                                    Output directory for processed CSV files
                                </div>
                            </li>
                            <li class="list-group-item d-flex justify-content-between align-items-start">
                                <div class="ms-2 me-auto">
                                    <div class="fw-bold"><code>etl/csv_runner.py</code></div>
                                    Simple CSV ETL implementation
                                </div>
                            </li>
                            <li class="list-group-item d-flex justify-content-between align-items-start">
                                <div class="ms-2 me-auto">
                                    <div class="fw-bold"><code>etl/ingest.py</code></div>
                                    Spark ETL implementation (reference)
                                </div>
                            </li>
                            <li class="list-group-item d-flex justify-content-between align-items-start">
                                <div class="ms-2 me-auto">
                                    <div class="fw-bold"><code>etl/expectations/</code></div>
                                    Great Expectations configuration (reference)
                                </div>
                            </li>
                        </ul>
                    </div>
                </div>
            </div>
        </div>
    </div>
    
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.2.3/dist/js/bootstrap.bundle.min.js"></script>
    <script>
    document.addEventListener('DOMContentLoaded', function() {
        const runButton = document.getElementById('run-etl-btn');
        const loadingIndicator = document.getElementById('loading');
        const resultsContainer = document.getElementById('results-container');
        
        runButton.addEventListener('click', function() {
            // Show loading indicator
            loadingIndicator.style.display = 'flex';
            runButton.disabled = true;
            
            // Make AJAX request
            fetch('/api/run-etl')
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        // Update results
                        document.getElementById('initial-count').textContent = data.statistics.initial_count;
                        document.getElementById('processed-count').textContent = data.statistics.transformed_count;
                        document.getElementById('filtered-count').textContent = data.statistics.filtered_count;
                        document.getElementById('output-path').textContent = data.output_path;
                        document.getElementById('timestamp').textContent = new Date(data.timestamp).toLocaleString();
                        
                        // Create sample data table
                        if (data.sample_rows && data.sample_rows.length > 0) {
                            const sampleHeader = document.getElementById('sample-header');
                            const sampleBody = document.getElementById('sample-body');
                            
                            // Clear previous data
                            sampleHeader.innerHTML = '';
                            sampleBody.innerHTML = '';
                            
                            // Create header
                            const headerRow = document.createElement('tr');
                            const firstRow = data.sample_rows[0];
                            Object.keys(firstRow).forEach(key => {
                                const th = document.createElement('th');
                                th.textContent = key;
                                headerRow.appendChild(th);
                            });
                            sampleHeader.appendChild(headerRow);
                            
                            // Create body rows
                            data.sample_rows.forEach(row => {
                                const tr = document.createElement('tr');
                                Object.values(row).forEach(value => {
                                    const td = document.createElement('td');
                                    td.textContent = value;
                                    tr.appendChild(td);
                                });
                                sampleBody.appendChild(tr);
                            });
                        }
                        
                        // Show results container
                        resultsContainer.style.display = 'block';
                    } else {
                        alert('ETL process failed: ' + data.error);
                    }
                })
                .catch(error => {
                    console.error('Error:', error);
                    alert('An error occurred while running the ETL process. Please check the console for details.');
                })
                .finally(() => {
                    // Hide loading indicator
                    loadingIndicator.style.display = 'none';
                    runButton.disabled = false;
                });
        });
    });
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    return render_template_string(
        html_template,
        alert_class="alert-info",
        status_message="ETL pipeline ready to run."
    )

@app.route('/api/run-etl')
def api_run_etl():
    try:
        from etl.csv_runner import run_csv_etl
        
        # Run ETL process
        logger.info("Starting ETL process")
        result = run_csv_etl()
        
        if result["success"]:
            logger.info("ETL process completed successfully")
            return jsonify(result)
        else:
            logger.error(f"ETL process failed: {result['error']}")
            return jsonify(result), 500
    
    except Exception as e:
        logger.exception("Error running ETL process")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/run-etl')
def run_etl():
    try:
        from etl.csv_runner import run_csv_etl
        
        # Run ETL process
        logger.info("Starting ETL process")
        result = run_csv_etl()
        
        if result["success"]:
            logger.info("ETL process completed successfully")
            stats = result["statistics"]
            message = f"ETL completed successfully! Processed {stats['transformed_count']} records (filtered {stats['filtered_count']}). Output: {result['output_path']}"
            return render_template_string(
                html_template,
                alert_class="alert-success",
                status_message=message
            )
        else:
            logger.error(f"ETL process failed: {result['error']}")
            return render_template_string(
                html_template,
                alert_class="alert-danger",
                status_message=f"ETL process failed. Error: {result['error']}"
            )
    except Exception as e:
        logger.exception("Error running ETL process")
        return render_template_string(
            html_template,
            alert_class="alert-danger",
            status_message=f"Error running ETL process: {str(e)}"
        )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)