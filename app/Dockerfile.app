# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /backend-agents/app

# Copy the current directory contents into the container at /backend-agents/app
COPY . /backend-agents/app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Add the parent directory to Python path
ENV PYTHONPATH=/backend-agents:$PYTHONPATH

ENV PYTHONUNBUFFERED=1

# Make port 8000 available to the world outside this container
EXPOSE 8000

# Run app.py when the container launches
CMD ["python", "-m", "uvicorn", "backend_app:app", "--host", "0.0.0.0", "--port", "8000","--workers","2"]