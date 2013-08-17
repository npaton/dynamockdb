package main

import (
	"fmt"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/dynamodb"
	"log"
	// "strings"
)

func main() {
	// This assumes you have ENV vars: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
	auth, err := aws.EnvAuth()
	if err != nil {
		log.Fatal(err.Error())
	}
	// aws.USEast.DynamoDBEndpoint = "http://localhost:3300"
	log.Printf("%+v", aws.USEast.DynamoDBEndpoint)
	server := dynamodb.Server{auth, aws.USEast}
	tables, err := server.ListTables()

	if err != nil {
		panic(err.Error())
	}

	if len(tables) == 0 {
		panic("Expected table to be returned")
	}

	fmt.Printf("tables %+v\n", tables)

	primary := dynamodb.NewStringAttribute("v", "")
	key := dynamodb.PrimaryKey{primary, nil}
	table := server.NewTable(tables[0], key)

	fmt.Printf("tables %+v\n", table)
	desc, err := table.DescribeTable()

	if err != nil {
		panic(err.Error())
	}

	if desc.TableSizeBytes > 0 {
		log.Println("TableSizeBytes > 0", desc.TableSizeBytes)
	}

	if desc.ItemCount > 0 {
		log.Println("ItemCount > 0", desc.ItemCount)
	}

	fmt.Printf("tables %+v\n", desc)
}
