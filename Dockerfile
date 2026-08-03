# Use a small Python 3.12 image as the base.
FROM python:3.12-slim

# All following commands run inside /app.
WORKDIR /app

# Install dependencies first, so Docker can reuse this step if only the code changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the app's code.
COPY . .

# Start the app. Shell form is used so $PORT is filled in when the container runs
# (Render sets $PORT itself; array form would not expand it).
CMD uvicorn main:app --host 0.0.0.0 --port $PORT