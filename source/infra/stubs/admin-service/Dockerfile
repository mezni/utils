FROM busybox:1.36
EXPOSE 3002
CMD ["busybox", "httpd", "-f", "-v", "-p", "3002", "-h", "/var/www"]
COPY index.html /var/www/index.html
