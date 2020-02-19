FROM bitmark/go-env:go12 AS builder

ADD go.mod /data-cleaner/
WORKDIR /data-cleaner
RUN go mod download


# Build server
ADD main.go /data-cleaner
RUN go build

# Actual image

FROM bitmark/go-env:go12

ENV SENTRY_ENV staging
ENV SENTRY_DSN ""

ENV BITSOCIAL_TOKEN ""
ENV BITSOCIAL_API_ENDPOINT https://bitsocial-test.bitmark.com/api/
ENV BITSOCIAL_BUCKET bitsocial-test
ENV BITSOCIAL_DB_URI "postgres://user:pass@host:port/database"

COPY --from=builder /data-cleaner/data-cleaner /data-cleaner

CMD ["/data-cleaner"]
