FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/reservation-api ./cmd/reservation-api \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/loadgen ./cmd/loadgen

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/reservation-api"]
