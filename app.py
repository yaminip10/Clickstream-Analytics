import json
import boto3
from flask import Flask, request, jsonify

# AWS configuration and replace with Kinesis Firehose delivery stream name
REGION_NAME = 'us-east-1'
DELIVERY_STREAM_NAME = "PUT-S3-Cst"

app = Flask(__name__)

@app.route("/collect", methods=["POST"])
def collect_event():
    # Get the request data
    data = request.get_json()

    if not data:
        return jsonify({"error": "Missing JSON data in request body"}), 400

    # Prepare the Kinesis Firehose client
    kinesis_client = boto3.client("firehose")
    
    # Convert data to bytes and prepare record
    data_bytes = json.dumps(data).encode("utf-8")
    record = {"Data": data_bytes}

    # Send data to Kinesis Firehose
    try:
        response = kinesis_client.put_record(DeliveryStreamName=DELIVERY_STREAM_NAME, Record=record)
        return jsonify({"message": "Event sent successfully"}), 201
    except Exception as e:
        print(f"Error sending data: {e}")
        return jsonify({"error": "Failed to send event"}), 500

if __name__ == "__main__":
    app.run(debug=True)
