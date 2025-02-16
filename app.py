import json
import logging
import os
from flask import Flask, request, jsonify
from task import process_weather_data, Config, GeographicProcessor

app = Flask(__name__)


# Endpoint to submit a weather processing task
@app.route('/weather', methods=['POST'])
def submit_weather_task():
    try:
        data = request.get_json()

        # Check city format
        if not data or 'cities' not in data:
            return jsonify({'error': 'Invalid request format'}), 400

        # Checking if input is list
        if not isinstance(data['cities'], list):
            return jsonify({'error': 'Cities must be provided as a list'}), 400

        geo_processor = GeographicProcessor()

        for city in data['cities']:
            if not isinstance(city, str) or not city.strip():
                return jsonify({'error': 'Invalid city name'}), 400

            # Checking if the city exists
            city_info = geo_processor.process_city(city)
            if not city_info:
                return jsonify({'error': f'City "{city}" does not exist'}), 400

        task = process_weather_data.delay(data['cities'])
        return jsonify({'task_id': task.id}), 202
    except Exception as e:
        logging.error(f"Error submitting task: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500


# Endpoint to check task status
@app.route('/tasks/<task_id>', methods=['GET'])
def get_task_status(task_id):
    try:
        task = process_weather_data.AsyncResult(task_id)
        response = {'status': task.status}

        if task.status == 'SUCCESS':
            response['results'] = task.get()
        elif task.status == 'FAILURE':
            response['error'] = str(task.result)

        return jsonify(response)
    except Exception as e:
        logging.error(f"Error checking task status: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500


# Endpoint to get weather results for a specific region
@app.route('/results/<region>', methods=['GET'])
def get_region_results(region):
    try:
        if region not in Config.VALID_REGIONS:
            return jsonify({'error': 'Invalid region'}), 400

        directory = os.path.join(Config.RESULTS_DIR, region)
        if not os.path.exists(directory):
            return jsonify({'results': []}), 200

        all_results = []
        for filename in os.listdir(directory):
            if filename.endswith('.json'):
                with open(os.path.join(directory, filename), 'r', encoding='utf-8') as f:
                    all_results.extend(json.load(f))

        return jsonify({'results': all_results})
    except Exception as e:
        logging.error(f"Error retrieving results: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500


if __name__ == '__main__':
    os.makedirs(Config.RESULTS_DIR, exist_ok=True)
    app.run(debug=True)
