FROM golang:1.26.9 AS source
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

FROM source AS build
RUN CGO_ENABLED=0 go build -o /out/ ./cmd/...

FROM alpine:3.20 AS controlplane
COPY --from=build /out/controlplane /usr/local/bin/controlplane
EXPOSE 8080
ENTRYPOINT ["controlplane"]

FROM alpine:3.20 AS worker
COPY --from=build /out/worker /usr/local/bin/worker
ENTRYPOINT ["worker"]

FROM alpine:3.20 AS inject
COPY --from=build /out/inject /usr/local/bin/inject
ENTRYPOINT ["inject"]

FROM source AS test
RUN go install golang.org/x/vuln/cmd/govulncheck@v1.1.4
CMD ["bash", "scripts/quality.sh"]
