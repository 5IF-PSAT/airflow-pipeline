host="$1"
port="$2"

response_code=$(curl -w "%{http_code}\n" -o /tmp/response.json "$host:$port/staging_full_weather/")

if [ "$response_code" -eq 200 ]; then
  echo "HTTP Status Code: 200 OK"
  echo "Response Body:"
  cat /tmp/response.json
  exit 0
else
  echo "HTTP Status Code: $response_code"
  exit 1
fi