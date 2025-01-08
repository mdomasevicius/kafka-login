package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/aws/aws-msk-iam-sasl-signer-go/signer"
	"os"
)

var (
	kafkaBrokers = []string{"<your_msk_bootstrap_string>"}
	KafkaTopic   = "<your topic name>"
	enqueued     int
)

type MSKAccessTokenProvider struct {
}

func (m *MSKAccessTokenProvider) Token() (*sarama.AccessToken, error) {
	token, _, err := signer.GenerateAuthToken(context.TODO(), "<region>")
	return &sarama.AccessToken{Token: token}, err
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("provide broker url")
		return
	}

	//region := "us-east-1"                                         // Replace with your region
	broker := os.Args[1] // Replace with your broker

	client, err := sarama.NewClient([]string{broker}, config())
	if err != nil {
		fmt.Println("Failed to create Kafka client: %v", err)
		return
	}
	defer client.Close()

	topics, err := client.Topics()
	if err != nil {
		fmt.Println("Failed to get topics: %v", err)
		return
	}
	fmt.Println("Kafka client successfully connected.")
	fmt.Println("Topics:")
	for i, topic := range topics {
		fmt.Println(fmt.Sprint(i) + " - " + topic)
	}
}

func config() *sarama.Config {
	// Set the SASL/OAUTHBEARER configuration
	c := sarama.NewConfig()
	c.Net.SASL.Enable = true
	c.Net.SASL.Mechanism = sarama.SASLTypeOAuth
	c.Net.SASL.TokenProvider = &MSKAccessTokenProvider{}

	tlsConfig := tls.Config{}
	c.Net.TLS.Enable = true
	c.Net.TLS.Config = &tlsConfig

	return c
}
