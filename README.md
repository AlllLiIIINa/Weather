# Asynchronous Weather Data Retrieval Task

## Getting Started

### 1. Clone the repository
``` git clone https://github.com/AlllLiIIINa/spy_agency.git ```

### 2. Install dependencies
```pip install -r requirements.txt```

### 3. Start Redis server

### 4. Start the Celery worker with the correct app name:
```celery -A task.celery worker --loglevel=info```

### 5. Run Flask application:
```python app.py```
