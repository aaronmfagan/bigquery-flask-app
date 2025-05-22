print("🚀 Flask app started")

from flask import Flask, request, jsonify
from google.cloud import bigquery
import tempfile, requests
import os

app = Flask(__name__)

@app.route('/init_bigquery', methods=['POST'])
def init_bigquery():
    data = request.json
    file_url = data.get("file_url")
    project_name = data.get("project_name")
    dataset_name = data.get("dataset_name")

    try:
        # Download JSON credentials file from Bubble
        r = requests.get(file_url)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as tmp:
            tmp.write(r.content)
            creds_path = tmp.name

        # Initialize BigQuery client
        client = bigquery.Client.from_service_account_json(creds_path, project=project_name)

        # Get schema from dataset
        tables_info = []
        tables = client.list_tables(dataset_name)
        for table in tables:
            schema = client.get_table(table).schema
            tables_info.append({
                "table": table.table_id,
                "columns": [{"name": col.name, "type": col.field_type} for col in schema]
            })

        return jsonify({"status": "success", "tables": tables_info})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"Running on host=0.0.0.0, port={port}")
    app.run(host="0.0.0.0", port=port, debug=True)