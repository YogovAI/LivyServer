from flask import Flask, request, jsonify
import requests
import json
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

LIVY_URL = "http://192.168.1.14:8998"

@app.route('/submit_job', methods=['POST'])
def submit_job():
    print("Entering here 1 : ")
    code = request.json.get('code')
    headers = {'Content-Type': 'application/json'}
    data = {
           "file": "/user/yogov/sample.py",  # Path to your job
        #    "className": "your.main.Class",  # Main class of your job
        #    "args": ["arg1", "arg2"],  # Arguments for your job
           "conf": {
               "spark.executor.memory": "2g"
           }
       }
    response = requests.post(f"{LIVY_URL}/batches", headers=headers, data=json.dumps(data))
    job_id = response.json().get('id')
    
    # Poll for job completion
    while True:
        # job_status = requests.get(f"{LIVY_URL}/batches/{job_id}").json()
        job_status = requests.get(f"{LIVY_URL}/batches/{job_id}").json()
        print("job status is : ", job_status['state'])
        if job_status['state'] in ['success', 'dead', 'failed']:
            break

    # Retrieve the result
    if job_status['state'] == 'success':
        data = request.json['data']
        print("Received data is : ", data)
        result_response = requests.get(f"{LIVY_URL}/batches/{job_id}/log").json()
        # result_response = requests.get(f"{LIVY_URL}/batches/{job_id}/result").json()
        print("Result is : ", requests.get(f"{LIVY_URL}/batches/{job_id}/log").json())
        return result_response #jsonify(result_response)
    else:
        return jsonify({"error": "Job failed", "details": job_status})
    # print("response is : ", response.json())
    # return jsonify(response.json())
    
    
@app.route('/receive_data', methods=['POST'])
def receive_data():
    data = request.json['data']
    print("Received data is : ", data)
    # Do something with the data (save it to a file or database)
    return jsonify({'status': 'received', 'data_length': len(data)})

if __name__ == '__main__':
    app.run(debug=True, port=5006)
