FROM golang:1.24 as tail_latency_build

WORKDIR /usr/src/app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -C tail_latency -v -o /usr/bin/app

FROM ubuntu:24.04 AS builder
RUN apt-get update -y && apt-get upgrade -y && \
apt-get install -y git g++ wget && \
wget https://go.dev/dl/go1.24.0.linux-amd64.tar.gz -O go.tar.gz && \
rm -rf /usr/local/go && tar -C /usr/local -xzf go.tar.gz
RUN git clone -b perf-disparity --depth 1  --single-branch https://github.com/GoogleCloudPlatform/gcsfuse.git
RUN cd gcsfuse && \
CGO_ENABLED=0 /usr/local/go/bin/go build .

FROM ubuntu:24.04
COPY --from=builder /gcsfuse/gcsfuse /gcsfuse
COPY --from=tail_latency_build /usr/bin/app /
RUN apt-get update -y && apt-get upgrade -y && \
apt-get install -y fuse3 apt-transport-https ca-certificates gnupg curl && \
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --batch --yes --dearmor -o /usr/share/keyrings/cloud.google.gpg && \
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
apt-get autoremove -y && \
apt-get clean

ENTRYPOINT ["/bin/bash"]