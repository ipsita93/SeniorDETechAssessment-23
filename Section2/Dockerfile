# Use the official PostgreSQL image from Docker Hub
FROM postgres:16.0

# Set environment variables for PostgreSQL
ENV POSTGRES_DB ecommerce_db
ENV POSTGRES_USER ecommerce_user
ENV POSTGRES_PASSWORD password123

#Set up labels
LABEL author="ipsita"
LABEL description="Postgres Image for e-commerce business"
LABEL version="1.0"

# Copy SQL script to initialize the database
COPY init.sql /docker-entrypoint-initdb.d/