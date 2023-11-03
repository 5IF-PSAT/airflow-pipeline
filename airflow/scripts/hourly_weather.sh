#!/bin/bash

host="$1"

port="$2"

topic_name="$3"
lat="$4"
lon="$5"
start_date="$6"
end_date="$7"
city="$8"

# Run the curl command and capture the status code in a variable
response_code=$(curl -s -o /dev/null -w "%{http_code}" "$host:$port/ingestion_weather/?topic_name=$topic_name&lat=$lat&lon=$lon&start_date=$start_date&end_date=$end_date&city=$city")
response=$(curl -s "$host:$port/ingestion_weather/?topic_name=$topic_name&lat=$lat&lon=$lon&start_date=$start_date&end_date=$end_date&city=$city")

# Check the value of the response_code
if [ "$response_code" -eq 200 ]; then
  echo "HTTP Status Code: 200 OK"
  echo "Response: $response"
  exit 0
else
  echo "HTTP Status Code: $response_code"
  exit 1
fi
