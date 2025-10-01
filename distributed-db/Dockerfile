# Use an official Go runtime as a parent image
FROM golang:1.18-alpine

# Set the working directory inside the container
WORKDIR /app

# Copy the local package files to the container's workspace
COPY . .

# Build the Go app. This command will build both router.go and database.go
# into executables named 'router' and 'database' inside the container.
RUN go build -o router router.go
RUN go build -o database database.go

# Note: The CMD will be specified in the Kubernetes manifest,
# as we need to run different executables with different flags.
