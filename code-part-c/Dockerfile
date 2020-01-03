FROM golang as builder

ENV GO11MODULE=on

WORKDIR /app

COPY go.mod .

COPY go.sum .

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build ./cmd/web

ENV USER=web


ENV PASS=test


ENV URL=db


ENV DB=hoppy


EXPOSE 3000

CMD ./web
