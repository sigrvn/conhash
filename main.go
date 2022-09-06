package main

import (
	"fmt"
)

var (
	servers = [5]string{"Spongebob", "Patrick", "Squidward", "Mr.Krabs", "Sandy"}
	hasher  = NewConsistentHasher()
)

func init() {
	for _, serverName := range servers {
		hasher.AddNode(serverName)
	}
	preorder(hasher.root)
}

func main() {
	// lambda.Start(Handler)
	testReq := Request{ID: "1134"}
	dest, err := Handler(testReq)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Sending Request %s to Server %s\n", testReq.ID, dest)
}

type Request struct {
	ID string `json:"id"`
}

func Handler(req Request) (string, error) {
	fmt.Printf("got request ID: %s\n", req.ID)
	return hasher.FindKey(req.ID)
}
