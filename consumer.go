package sqsconsumer

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type CallbackFunc func(*sqs.Message) error

type SQSConsumer struct {
	svc            *sqs.SQS
	queueURL       string
	messageGroupID string
}

func NewSQSConsumer(queueURL, accessKeyID, secretAccessKey, region string, messageGroupID string) (*SQSConsumer, error) {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewStaticCredentials(accessKeyID, secretAccessKey, ""),
	})
	if err != nil {
		return nil, err
	}

	return &SQSConsumer{
		svc:            sqs.New(sess),
		queueURL:       queueURL,
		messageGroupID: messageGroupID,
	}, nil
}

func (c *SQSConsumer) Consume(callback CallbackFunc) {
	for {
		result, err := c.svc.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueUrl:              aws.String(c.queueURL),
			MaxNumberOfMessages:   aws.Int64(1),
			WaitTimeSeconds:       aws.Int64(20),
			AttributeNames:        aws.StringSlice([]string{"MessageGroupId"}),
			MessageAttributeNames: aws.StringSlice([]string{"All"}),
		})

		if err != nil {
			log.Fatalf("Failed to receive message from queue: %v", err)
		}

		if len(result.Messages) > 0 {
			for _, message := range result.Messages {
				// Check if the message has the desired MessageGroupId
				if message.MessageAttributes["MessageGroupId"].StringValue != nil &&
					*message.MessageAttributes["MessageGroupId"].StringValue == c.messageGroupID {
					err = callback(message)
					if err != nil {
						log.Printf("Error processing message: %v", err)
					}

					// Delete the message from the queue
					_, err = c.svc.DeleteMessage(&sqs.DeleteMessageInput{
						QueueUrl:      aws.String(c.queueURL),
						ReceiptHandle: message.ReceiptHandle,
					})
					if err != nil {
						log.Printf("Failed to delete message from queue: %v", err)
					}
				}
			}
		} else {
			log.Println("No messages in the queue with the specified MessageGroupId")
		}
	}
}
func StartConsumer(consumer *SQSConsumer, callback CallbackFunc) {
	go consumer.Consume(callback)

	log.Println("Consumer started. Press CTRL+C to exit.")

	// Wait for termination signal
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig

	log.Println("Terminating...")
}
