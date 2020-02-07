package storage

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type SQS struct {
	svc      *sqs.SQS
	queueURL string
}

func NewSQS(sess *session.Session, queueURL string) *SQS {
	svc := sqs.New(sess)
	return &SQS{svc, queueURL}
}

func (s *SQS) Poll() (*sqs.ReceiveMessageOutput, error) {
	input := &sqs.ReceiveMessageInput{
		QueueUrl:        aws.String(s.queueURL),
		WaitTimeSeconds: aws.Int64(20),
	}
	return s.svc.ReceiveMessage(input)
}

func (s *SQS) DeleteMessage(m *sqs.Message) {
	input := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(s.queueURL),
		ReceiptHandle: m.ReceiptHandle,
	}
	s.svc.DeleteMessage(input)
}
