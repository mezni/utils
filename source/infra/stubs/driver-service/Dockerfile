FROM busybox:1.36
EXPOSE 3001
CMD ["busybox", "httpd", "-f", "-v", "-p", "3001", "-h", "/var/www"]
COPY index.html /var/www/index.html
