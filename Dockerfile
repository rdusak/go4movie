# syntax=docker/dockerfile:1
FROM golang:1.17-alpine
WORKDIR /app
RUN apk update && apk upgrade
RUN apk add --no-cache sqlite 
RUN apk add --no-cache gcc musl-dev
COPY go.mod ./
COPY go.sum ./
RUN export CGO_ENABLED=1
RUN go mod download
RUN go install github.com/mattn/go-sqlite3
COPY *.go ./
RUN go build -o /main main.go
EXPOSE 8080
CMD [ "/main" ] 