# producer/Dockerfile
FROM golang:1.19-alpine

# Set the working directory inside the container
WORKDIR /app

# Copy the Go source code into the container
COPY . .

# Build the Go application
RUN go build -o producer .

# Expose port 8080 for the telemetry producer
EXPOSE 8080

# Start the producer service
CMD ["/app/producer"]
