FROM bitmark/go-env:go12

WORKDIR /go-mod/github.com/datapod/data-parser

ADD go.mod .
ADD go.sum .

RUN go mod download

ADD . .

RUN go install github.com/bitmark-inc/datapod/data-parser

CMD data-parser
