# Start from the official Go image
FROM golang:1.22-alpine

# Set the working directory inside the container
WORKDIR /app

# Copy the go.mod and go.sum files first (if you have them)
# COPY go.mod go.sum ./
# RUN go mod download

# Copy the entire mono-repo context so the gateway can access the shared proto files
COPY . .

# Change to the gateway directory and build the binary
WORKDIR /app/gateway
RUN go build -o api-gateway main.go

# Expose the REST and gRPC ports defined in the architecture map
EXPOSE 8080 9090

# Command to run the executable
CMD ["./api-gateway"]