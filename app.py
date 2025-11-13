# # requirements.txt
# flask==3.0.0
# azure-eventhub==5.11.6

# app.py
import os
import json
from datetime import datetime
from flask import Flask, request, jsonify, render_template_string
from azure.eventhub import EventHubProducerClient, EventData
import random

app = Flask(__name__)

# Get connection string from environment
connection_str = os.environ.get('EventHubConnectionString')

# Extract entity path from connection string
def get_entity_path(conn_str):
    if not conn_str:
        return None
    parts = conn_str.split(';')
    for part in parts:
        if part.startswith('EntityPath='):
            return part.split('=')[1]
    return None

entity_path = get_entity_path(connection_str)

HTML_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>Fabric Event Stream Sender</title>
    <style>
        body { 
            font-family: Arial, sans-serif; 
            max-width: 800px; 
            margin: 50px auto; 
            padding: 20px;
            background: #f5f5f5;
        }
        .container {
            background: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        h1 { color: #0078d4; }
        button {
            background: #0078d4;
            color: white;
            border: none;
            padding: 12px 24px;
            font-size: 16px;
            border-radius: 4px;
            cursor: pointer;
            margin: 10px 5px;
        }
        button:hover { background: #005a9e; }
        #status {
            margin-top: 20px;
            padding: 15px;
            border-radius: 4px;
            display: none;
        }
        .success { background: #dff0d8; color: #3c763d; display: block; }
        .error { background: #f2dede; color: #a94442; display: block; }
        .info { background: #d9edf7; color: #31708f; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🐍 Python App Service → Fabric Event Stream</h1>
        <p>Send test events to Microsoft Fabric Event Stream from Python.</p>
        
        <div class="info" style="display: block; margin: 20px 0;">
            <strong>Configuration:</strong><br>
            Entity Path: <code>{{ entity_path or 'Not configured' }}</code><br>
            Connected: <code>{{ 'Yes' if connection_str else 'No' }}</code>
        </div>

        <button onclick="sendEvent('single')">Send Single Event</button>
        <button onclick="sendEvent('batch')">Send 10 Events</button>
        <button onclick="sendEvent('sensor')">Send Sensor Data</button>
        
        <div id="status"></div>
    </div>

    <script>
        async function sendEvent(type) {
            const statusDiv = document.getElementById('status');
            statusDiv.textContent = 'Sending...';
            statusDiv.className = 'info';
            statusDiv.style.display = 'block';
            
            try {
                const response = await fetch('/api/send/' + type, {
                    method: 'POST'
                });
                const data = await response.json();
                
                if (data.success) {
                    statusDiv.textContent = '✓ ' + data.message;
                    statusDiv.className = 'success';
                } else {
                    statusDiv.textContent = '✗ Error: ' + data.error;
                    statusDiv.className = 'error';
                }
            } catch (error) {
                statusDiv.textContent = '✗ Error: ' + error.message;
                statusDiv.className = 'error';
            }
        }
    </script>
</body>
</html>
'''

@app.route('/')
def home():
    return render_template_string(
        HTML_TEMPLATE, 
        connection_str=connection_str, 
        entity_path=entity_path
    )

@app.route('/api/send/single', methods=['POST'])
def send_single_event():
    try:
        producer = EventHubProducerClient.from_connection_string(
            conn_str=connection_str
        )
        
        event_data_batch = producer.create_batch()
        event = {
            'message': 'Test event from Python App Service',
            'timestamp': datetime.now().isoformat(),
            'source': 'Python-Flask'
        }
        
        event_data_batch.add(EventData(json.dumps(event)))
        producer.send_batch(event_data_batch)
        producer.close()
        
        print(f'Event sent: {event}')
        return jsonify({
            'success': True, 
            'message': 'Event sent successfully to Fabric Event Stream'
        })
    except Exception as e:
        print(f'Error sending event: {str(e)}')
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/send/batch', methods=['POST'])
def send_batch_events():
    try:
        producer = EventHubProducerClient.from_connection_string(
            conn_str=connection_str
        )
        
        event_data_batch = producer.create_batch()
        
        for i in range(1, 11):
            event = {
                'eventId': i,
                'message': f'Batch event {i}',
                'timestamp': datetime.now().isoformat(),
                'value': round(random.random() * 100, 2)
            }
            event_data_batch.add(EventData(json.dumps(event)))
        
        producer.send_batch(event_data_batch)
        producer.close()
        
        print('Batch of 10 events sent successfully')
        return jsonify({
            'success': True, 
            'message': '10 events sent successfully to Fabric Event Stream'
        })
    except Exception as e:
        print(f'Error sending batch: {str(e)}')
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/send/sensor', methods=['POST'])
def send_sensor_data():
    try:
        producer = EventHubProducerClient.from_connection_string(
            conn_str=connection_str
        )
        
        event_data_batch = producer.create_batch()
        
        # Simulate 5 IoT devices
        for device_id in range(1, 6):
            sensor_data = {
                'deviceId': f'device-{device_id}',
                'temperature': round(random.uniform(20, 35), 2),
                'humidity': round(random.uniform(40, 70), 2),
                'pressure': round(random.uniform(980, 1030), 2),
                'timestamp': datetime.now().isoformat(),
                'location': f'zone-{device_id}'
            }
            event_data_batch.add(EventData(json.dumps(sensor_data)))
        
        producer.send_batch(event_data_batch)
        producer.close()
        
        print('Sensor data sent successfully')
        return jsonify({
            'success': True, 
            'message': 'Sensor data sent successfully to Fabric Event Stream'
        })
    except Exception as e:
        print(f'Error sending sensor data: {str(e)}')
        return jsonify({'success': False, 'error': str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    app.run(host='0.0.0.0', port=port, debug=False)
