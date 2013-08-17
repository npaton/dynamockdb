package dynamockdb

// import (
// 	"encoding/json"
// 	"io"
// 	"log"
// 	"net/http"
// 	"strings"
// )

// var db *DB

// func main() {
// 	http.HandleFunc("/", Handler)

// 	db = NewDB()
// 	req := &CreateTableRequest{
// 		AttributeDefinitions:  []AttributeDefinition{AttributeDefinition{AttributeName: "foo", AttributeType: StringAttributeType}},
// 		KeySchema:             []KeySchemaElement{KeySchemaElement{AttributeName: "id", KeyType: HashKeyType}},
// 		ProvisionedThroughput: ProvisionedThroughput{ReadCapacityUnits: 5, WriteCapacityUnits: 5},
// 		TableName:             "bar",
// 	}
// 	db.CreateTable(req)

// 	log.Println("Starting dynamockdb")
// 	log.Fatal(http.ListenAndServe(":3000", nil))
// }

// func Handler(w http.ResponseWriter, r *http.Request) {

// 	dec := json.NewDecoder(r.Body)
// 	enc := json.NewEncoder(w)

// 	// buf := make([]byte, 10000)
// 	// n, err := r.Body.Read(buf)
// 	// if err != nil {
// 	// 	panic(err)
// 	// }
// 	// log.Println(string(buf[:n]))

// 	if r.Header["X-Amz-Target"] == nil {
// 		http.Error(w, "Missing X-Amz-Target", 400)
// 		return
// 	}

// 	t := strings.Split(r.Header["X-Amz-Target"][0], ".")
// 	cmd := t[len(t)-1:][0]
// 	switch cmd {
// 	case "ListTables":
// 		req := &ListTablesRequest{}
// 		err := dec.Decode(req)
// 		if err != nil {
// 			if err != io.EOF {
// 				panic(err)
// 			}
// 		}
// 		log.Printf("ListTables %#+v", req)
// 		res := db.ListTables(req)
// 		log.Printf("%#+v", res)
		
// 		err = enc.Encode(res)
// 		if err != nil {
// 			panic(err)
// 		}
// 	case "DescribeTable":
// 		req := &DescribeTableRequest{}
// 		err := dec.Decode(req)
// 		if err != nil {
// 			if err != io.EOF {
// 				panic(err)
// 			}
// 		}
// 		log.Printf("Describe: %#+v", req.TableName)
// 		res := db.DescribeTable(req)
// 		log.Printf("%#+v", res)
		
// 		err = enc.Encode(res)
// 		if err != nil {
// 			panic(err)
// 		}
// 	default:
// 		http.Error(w, "Uknown X-Amz-Target", 400)
// 		return
// 	}
// 	// dec := json.NewDecoder(r.Body)
// 	// dec.Decode(v)
// }
