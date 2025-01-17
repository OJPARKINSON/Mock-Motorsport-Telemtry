# consumer/Dockerfile
FROM golang:1.19-alpine

# Set the working directory inside the container
WORKDIR /app

# Copy the Go source code into the container
COPY . .

# Build the Go application
RUN go build -o consumer .

# Expose port 8081 for the consumer
EXPOSE 8081

# Start the consumer service
CMD ["/app/consumer"]