import React, { useState } from 'react';

function App() {
  const [jobStatus, setJobStatus] = useState('');

  const submitJob = async () => {
    try {
      const response = await fetch('http://localhost:5006/submit_job', {
        method: 'POST',  // Ensure this is the correct method
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ jobData: 'Sample job data' })
      });

      if (response.ok) {
        const result = await response.json();
        console.log("Spark Job submitted successfully....")
        setJobStatus(result.status);
      } else {
        setJobStatus('Failed to submit job');
      }
    } catch (error) {
      setJobStatus('Error: ' + error.message);
    }
  };

  return (
    <div>
      <h1>Submit Job</h1>
      <button onClick={submitJob}>Submit</button>
      <p>{jobStatus}</p>
    </div>
  );
}

export default App;
