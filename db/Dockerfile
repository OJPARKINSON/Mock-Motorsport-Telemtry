# db/Dockerfile
FROM postgres:14

# Copy the initialization SQL script to Docker’s entrypoint path
COPY init.sql /docker-entrypoint-initdb.d/

# Expose PostgreSQL's default port
EXPOSE 5432