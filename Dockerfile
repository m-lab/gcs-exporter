FROM golang:1.13 as build
ENV CGO_ENABLED 0
ENV GOPATH /go
COPY . /go/src/github.com/m-lab/gcs-exporter/
# Build and put the git commit hash into the binary.
RUN go get \
      -v \
      -ldflags "-X github.com/m-lab/go/prometheusx.GitShortCommit=$(git log -1 --format=%h)" \
      github.com/m-lab/gcs-exporter


# Now copy the cbif binary into a minimal base image.
FROM alpine
# Download but prevent saving APKINDEX files with -no-cache.
RUN apk add --no-cache ca-certificates
COPY --from=build /go/bin/gcs-exporter /
ENV PATH /:$PATH
ENTRYPOINT ["/gcs-exporter"]
