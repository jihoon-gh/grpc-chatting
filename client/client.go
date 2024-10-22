package main

import (
	"bufio"
	pb "chattingbygrpc/chatproto"
	"context"
	"flag"
	"io"
	"log"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var id = flag.String("id", "unknown", "The id name")
var serverAddr = flag.String("addr", "localhost:50051", "The server address in the format of host:port")

func main() {
	flag.Parse()
	conn, err := grpc.Dial(*serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()
	client := pb.NewChatServiceClient(conn)

	runChat(client)
}

func runChat(client pb.ChatServiceClient) {
	stream, err := client.Chat(context.Background())
	if err != nil {
		log.Fatalf("client.Char failed: %v", err)
	}
	waitc := make(chan struct{})

	go func() {
		for {
			in, err := stream.Recv()
			if err == io.EOF {
				close(waitc)
				return
			}
			if err != nil {
				log.Fatalf("client.Chat failed: %v", err)
			}
			log.Printf("Sender:%s Message:%s", in.Sender, in.Message)
		}
	}()
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		msg := scanner.Text()
		if strings.ToLower(msg) == "exit" {
			break
		}

		stream.Send(&pb.ChatMsg{
			Sender:  *id,
			Message: msg,
		})
	}
	stream.CloseSend()
	<-waitc
}
