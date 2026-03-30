"""
Flask app: serves the dashboard and runs the data pipeline on demand.
"""

import os
import json
import threading
from datetime import datetime
from flask import Flask, jsonify, render_template, send_from_directory

app = Flask(__name__)

# In-memory state
DATA_FILE = os.path.join(os.path.dirname(__file__), 'data.json')
pipeline_status = {
    'running': False,
    'progress': [],
    'last_error': None,
}


def load_data():
    """Load cached data from disk."""
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE) as f:
            return json.load(f)
    return None


def run_pipeline_async():
    """Run the pipeline in a background thread."""
    from pipeline import run_pipeline

    pipeline_status['running'] = True
    pipeline_status['progress'] = []
    pipeline_status['last_error'] = None

    def on_progress(msg):
        pipeline_status['progress'].append(msg)

    try:
        result = run_pipeline(progress_cb=on_progress)
        with open(DATA_FILE, 'w') as f:
            json.dump(result, f)
        pipeline_status['running'] = False
    except Exception as e:
        pipeline_status['last_error'] = str(e)
        pipeline_status['running'] = False


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/data')
def api_data():
    """Return the most recent cached data."""
    data = load_data()
    if data:
        return jsonify(data)
    return jsonify({'error': 'No data yet. Click Refresh to run the pipeline.'}), 404


@app.route('/api/refresh', methods=['POST'])
def api_refresh():
    """Trigger a pipeline run in the background."""
    if pipeline_status['running']:
        return jsonify({'status': 'already_running', 'progress': pipeline_status['progress']})

    t = threading.Thread(target=run_pipeline_async, daemon=True)
    t.start()
    return jsonify({'status': 'started'})


@app.route('/api/status')
def api_status():
    """Check pipeline progress."""
    return jsonify({
        'running': pipeline_status['running'],
        'progress': pipeline_status['progress'],
        'error': pipeline_status['last_error'],
    })


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
