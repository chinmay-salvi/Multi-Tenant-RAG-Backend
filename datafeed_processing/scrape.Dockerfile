# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /usr/src

RUN mkdir datafeed_processing

# Copy the requirements file into the container
COPY datafeed_processing/scrape_requirements.txt datafeed_processing/scrape_requirements.txt

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r datafeed_processing/scrape_requirements.txt

COPY datafeed_processing/tables.py datafeed_processing/tables.py

# Copy the rest of the script into the container
COPY datafeed_processing/scrape_urls.py datafeed_processing/scrape_urls.py

# Command to run the application
CMD ["python3", "-u","datafeed_processing/scrape_urls.py"]