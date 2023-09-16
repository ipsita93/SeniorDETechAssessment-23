# Section 1: Data Pipelines

## Build the Docker image
docker build -t ecommerce-postgres .

## Run the Docker container
docker run -d --name ecommerce-db -p 5432:5432 ecommerce-postgres
