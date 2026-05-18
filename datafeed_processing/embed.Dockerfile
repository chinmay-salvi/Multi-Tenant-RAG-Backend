# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /usr/src

RUN mkdir datafeed_processing

# Copy the requirements file into the container
COPY datafeed_processing/embed_requirements.txt datafeed_processing/embed_requirements.txt

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r datafeed_processing/embed_requirements.txt

COPY datafeed_processing/plans_helper.py datafeed_processing/plans_helper.py

COPY datafeed_processing/tables.py datafeed_processing/tables.py

COPY datafeed_processing/embed_datafeeds.py datafeed_processing/embed_datafeeds.py

# Command to run the application
CMD ["python3", "-u", "datafeed_processing/embed_datafeeds.py"]