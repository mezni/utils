#!/bin/sh
RESPONSE="HTTP/1.0 404 Not Found\r\nContent-Type: application/json\r\n\r\n{\"error\":\"route_not_found\"}\r\n"
while true; do
  echo -e "$RESPONSE" | nc -l -p 3099 -w 2 > /dev/null 2>&1
done
