FROM golang:1.13 as build
ENV CGO_ENABLED 0
ENV GOPATH /go
COPY . /go/src/github.com/stephen-soltesz/cloud-build-if/
RUN go get -v github.com/stephen-soltesz/cloud-build-if/cmd/cbif

# Now copy the cbif binary into a minimal base image.
FROM scratch
COPY --from=build /go/bin/cbif /
ENV PATH /:$PATH
ENTRYPOINT ["/cbif"]
