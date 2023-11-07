#!/bin/bash

host="$1"

port="$2"
lat="$3"
lon="$4"
start_date="$5"
end_date="$6"
file_name="$7"
city="$8"

# Run the curl command and capture the status code in a variable
response_code=$(curl -s -o /dev/null -w "%{http_code}" "$host:$port/ingestion_weather/?lat=$lat&lon=$lon&start_date=$start_date&end_date=$end_date&file_name=$file_name&city=$city&daily=yes")
response=$(curl -s "$host:$port/ingestion_weather/?lat=$lat&lon=$lon&start_date=$start_date&end_date=$end_date&file_name=$file_name&city=$city&daily=yes")

# Check the value of the response_code
if [ "$response_code" -eq 200 ]; then
  echo "HTTP Status Code: 200 OK"
  echo "Response: $response"
  exit 0
else
  echo "HTTP Status Code: $response_code"
  exit 1
fi
