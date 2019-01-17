# Build the manager binary
FROM golang:1.11.4-alpine as builder

# Copy in the go src
WORKDIR /go/src/github.com/containers-ai/karina
ADD . .

# Build
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o ./operator/manager github.com/containers-ai/karina/operator/cmd/manager

# Copy the controller-manager into a thin image
FROM alpine:latest
WORKDIR /root/
COPY --from=builder /go/src/github.com/containers-ai/karina/operator/etc/operator.yml /etc/federatorai/operator/operator.yml
COPY --from=builder /go/src/github.com/containers-ai/karina/operator/manager .
EXPOSE 8080/tcp
ENTRYPOINT ["./manager"]
