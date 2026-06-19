FROM busybox:1.36
EXPOSE 3000
CMD ["busybox", "httpd", "-f", "-v", "-p", "3000", "-h", "/var/www"]
COPY index.html /var/www/index.html
