FROM semtech/mu-javascript-template:1.8.0

LABEL maintainer="info@redpencil.io"

ENV EXPORT_CRON_PATTERN '0 0 */2 * * *'
ENV EXPORT_FILE_BASE mandaten
ENV EXPORT_TTL_BATCH_SIZE 1000
