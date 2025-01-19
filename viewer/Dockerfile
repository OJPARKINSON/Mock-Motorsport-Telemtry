# viewer/Dockerfile
FROM golang:1.19-alpine

# Set the working directory inside the container
WORKDIR /app

# Copy the Go source code into the container
COPY . .

# Build the Go application
RUN go build -o viewer .

# Expose port 8082 for the viewer
EXPOSE 8082

# Start the viewer service
CMD ["/app/viewer"]