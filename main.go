package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
)

func main() {
	open, err := os.Open("config.json")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer open.Close()

	var cfg RestClientConfig
	err = json.NewDecoder(open).Decode(&cfg)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	client, err := NewRestClient(cfg)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	quote, err := client.LoopOutQuote(context.Background(), "50000000")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println(fmt.Sprintf("%+v", quote))
}
