FROM golang:1.24 as build

WORKDIR /usr/src/app
COPY . .
RUN go mod download
RUN CGO_ENABLED=0 go build -C tail_latency -v -o /usr/bin/app

# Now copy it into our base image.
FROM gcr.io/distroless/static-debian12
COPY --from=build /usr/bin/app /
CMD ["/app"]