package main

import (
	"testing"
	"time"
)

func TestPutItem(t *testing.T) {
	db := NewDB()
	CreateTable(db, "bax")
	table := db.GetTable("bax")

	// Creating

	req := &PutItemRequest{
		Item:         map[string]AttributeValue{"id": AttributeValue{S: "bar"}, "foo": AttributeValue{S: "baz"}},
		TableName:    "bax",
		ReturnValues: AllNewReturnValues,
	}

	result, _ := table.PutItem(req)
	if result == nil {
		t.Fatalf("", result)
	}

	if result.Attributes["foo"].S != "baz" && result.Attributes["id"].S != "bar" {
		t.Fatalf("", result)
	}

	// Updating

	req = &PutItemRequest{
		Item:         map[string]AttributeValue{"id": AttributeValue{S: "bar"}, "foo": AttributeValue{S: "boz"}},
		TableName:    "bax",
		ReturnValues: AllNewReturnValues,
	}

	result, _ = table.PutItem(req)
	if result == nil {
		t.Fatalf("", result)
	}

	if result.Attributes["foo"].S != "boz" {
		t.Fatalf("", result)
	}

	// UpdatedOldReturnValues

	req = &PutItemRequest{
		Item:         map[string]AttributeValue{"id": AttributeValue{S: "bar"}, "foo": AttributeValue{S: "boom"}},
		TableName:    "bax",
		ReturnValues: UpdatedOldReturnValues,
	}

	result, _ = table.PutItem(req)
	if result == nil {
		t.Fatalf("", result)
	}

	if result.Attributes["foo"].S != "boz" {
		t.Fatalf("", result)
	}

	// Expected Exist

	req = &PutItemRequest{
		Item:         map[string]AttributeValue{"id": AttributeValue{S: "bar"}, "foo": AttributeValue{S: "boom"}, "four": AttributeValue{S: "boom"}},
		TableName:    "bax",
		ReturnValues: UpdatedOldReturnValues,
		Expected:     map[string]ExpectedAttributeValue{"foo": ExpectedAttributeValue{Exists: false}},
	}

	result, err := table.PutItem(req)
	if err == nil {
		t.Fatalf("", result)
	}

	// Expected AttributeValue

	req = &PutItemRequest{
		Item:         map[string]AttributeValue{"id": AttributeValue{S: "bar"}, "foo": AttributeValue{S: "bam"}},
		TableName:    "bax",
		ReturnValues: UpdatedNewReturnValues,
		Expected:     map[string]ExpectedAttributeValue{"foo": ExpectedAttributeValue{Exists: true, Value: AttributeValue{S: "bom"}}},
	}

	result, err = table.PutItem(req)
	if err == nil {
		t.Fatalf("", result)
	}
}

func TestUpdateItem(t *testing.T) {
	db := NewDB()
	CreateTable(db, "bax")
	table := db.GetTable("bax")
	InsertItem(table, "bax", map[string]AttributeValue{"id": AttributeValue{S: "bar"}, "foo": AttributeValue{S: "bam"}, "bar": AttributeValue{S: "baz"}})

	// Updating

	req := &UpdateItemRequest{
		Key:              map[string]AttributeValue{"id": AttributeValue{S: "bar"}},
		AttributeUpdates: map[string]AttributeValueUpdate{"foo": AttributeValueUpdate{Action: PutUpdateAction, Value: AttributeValue{S: "bar"}}},
		TableName:        "bax",
		ReturnValues:     AllNewReturnValues,
	}

	result, _ := table.UpdateItem(req)
	if result == nil {
		t.Fail()
	}

	if result.Attributes["foo"].S != "bar" && result.Attributes["bar"].S != "baz" {
		t.Fail()
	}

	// Deleting

	req = &UpdateItemRequest{
		Key:              map[string]AttributeValue{"id": AttributeValue{S: "bar"}},
		AttributeUpdates: map[string]AttributeValueUpdate{"foo": AttributeValueUpdate{Action: DeleteUpdateAction}},
		TableName:        "bax",
		ReturnValues:     AllNewReturnValues,
	}

	result, _ = table.UpdateItem(req)
	if result == nil {
		t.Fail()
	}

	if result.Attributes["foo"].S == "bar" && result.Attributes["bar"].S != "baz" {
		t.Fail()
	}
}

func TestGetItem(t *testing.T) {
	db := NewDB()
	CreateTable(db, "bax")
	table := db.GetTable("bax")
	InsertItem(table, "bax", map[string]AttributeValue{"id": AttributeValue{S: "bar"}, "foo": AttributeValue{S: "bam"}})
	InsertItem(table, "bax", map[string]AttributeValue{"id": AttributeValue{S: "ba"}, "foo": AttributeValue{S: "bar"}})

	// Get all fields

	req := &GetItemRequest{
		Key:             map[string]AttributeValue{"id": AttributeValue{S: "bar"}},
		TableName:       "bax",
		AttributesToGet: []string{"id", "foo"},
	}

	result, _ := table.GetItem(req)
	if result == nil {
		t.Fail()
	}

	if result.Item["id"].S != "bar" && result.Item["foo"].S != "bam" {
		t.Fail()
	}

	// Get all fields by default

	req = &GetItemRequest{
		Key:       map[string]AttributeValue{"id": AttributeValue{S: "bar"}},
		TableName: "bax",
	}

	result, _ = table.GetItem(req)
	if result == nil {
		t.Fail()
	}

	if result.Item["id"].S != "bar" && result.Item["foo"].S != "bam" {
		t.Fail()
	}

	// Get only certain attributes

	req = &GetItemRequest{
		Key:             map[string]AttributeValue{"id": AttributeValue{S: "bar"}},
		TableName:       "bax",
		AttributesToGet: []string{"foo"},
	}

	result, _ = table.GetItem(req)
	if result == nil {
		t.Fail()
	}

	// id not there anymore
	if result.Item["id"].S == "bar" && result.Item["foo"].S != "bam" {
		t.Fail()
	}
}

func TestDeleteItem(t *testing.T) {
	db := NewDB()
	CreateTable(db, "bax")
	table := db.GetTable("bax")
	InsertItem(table, "bax", map[string]AttributeValue{"id": AttributeValue{S: "bar"}, "foo": AttributeValue{S: "bam"}})
	InsertItem(table, "bax", map[string]AttributeValue{"id": AttributeValue{S: "ba"}, "foot": AttributeValue{S: "bar"}})

	// Get all fields

	req := &DeleteItemRequest{
		Key: map[string]AttributeValue{"id": AttributeValue{S: "bar"}},
	}

	result, _ := table.DeleteItem(req)
	if result == nil {
		t.Fail()
	}

	if result.Attributes["foo"].S != "bam" && result.Attributes["id"].S != "bar" {
		t.Fail()
	}

	// Deleted item

	reqB := &GetItemRequest{
		Key:             map[string]AttributeValue{"id": AttributeValue{S: "bar"}},
		TableName:       "bax",
		AttributesToGet: []string{"foo"},
	}

	_, err := table.GetItem(reqB)
	if err == nil {
		t.Fatalf("err should be nil")
	}

	// Other still there

	reqB = &GetItemRequest{
		Key:             map[string]AttributeValue{"id": AttributeValue{S: "ba"}},
		TableName:       "bax",
		AttributesToGet: []string{"foot"},
	}

	resultB, err := table.GetItem(reqB)
	if resultB.Item["foot"].S != "bar" && resultB.Item["id"].S != "ba" {
		t.Fatalf("wrong values", resultB.Item)
	}

	// Delete other

	req = &DeleteItemRequest{
		Key: map[string]AttributeValue{"id": AttributeValue{S: "ba"}},
	}

	result, _ = table.DeleteItem(req)
	if result == nil {
		t.Fail()
	}

	if result.Attributes["foot"].S != "bar" && result.Attributes["id"].S != "ba" {
		t.Fail()
	}

	// Delete nothing

	req = &DeleteItemRequest{
		Key: map[string]AttributeValue{"id": AttributeValue{S: "nothing"}},
	}

	_, err = table.DeleteItem(req)
	if err == nil {
		t.Fail()
	}

}

func TestUpdateTable(t *testing.T) {
	db := NewDB()

	CreateTable(db, "bax")
	table := db.GetTable("bax")

	req := &CreateTableRequest{
		AttributeDefinitions:  []AttributeDefinition{AttributeDefinition{AttributeName: "foo", AttributeType: StringAttributeType}},
		KeySchema:             []KeySchemaElement{KeySchemaElement{AttributeName: "id", KeyType: HashKeyType}},
		ProvisionedThroughput: ProvisionedThroughput{ReadCapacityUnits: 5, WriteCapacityUnits: 5},
		TableName:             "bar",
	}
	tableDesc := db.CreateTable(req)

	if tableDesc.TableDescription.ProvisionedThroughput.ReadCapacityUnits != 5 {
		t.Fatalf("", tableDesc.TableDescription.ProvisionedThroughput.ReadCapacityUnits)
	}

	if time.Since(tableDesc.TableDescription.ProvisionedThroughput.LastIncreaseDateTime) > 1*time.Millisecond {
		t.Fatalf("", tableDesc.TableDescription.ProvisionedThroughput.LastIncreaseDateTime)
	}

	reqB := &UpdateTableRequest{
		ProvisionedThroughput: ProvisionedThroughput{ReadCapacityUnits: 15, WriteCapacityUnits: 15},
		TableName:             "bar",
	}
	tableUpdateDesc, err := table.UpdateTable(reqB)
	if err != nil {
		t.Fatalf(err.Error())
	}

	if tableUpdateDesc.TableDescription.ProvisionedThroughput.ReadCapacityUnits != 15 {
		t.Fatalf("", tableUpdateDesc.TableDescription.ProvisionedThroughput.ReadCapacityUnits)
	}

	if time.Since(tableUpdateDesc.TableDescription.ProvisionedThroughput.LastIncreaseDateTime) > 1*time.Millisecond {
		t.Fatalf("", tableUpdateDesc.TableDescription.ProvisionedThroughput.ReadCapacityUnits)
	}

	reqB = &UpdateTableRequest{
		ProvisionedThroughput: ProvisionedThroughput{ReadCapacityUnits: 10, WriteCapacityUnits: 15},
		TableName:             "bar",
	}
	tableUpdateDesc, err = table.UpdateTable(reqB)
	if err != nil {
		t.Fatalf(err.Error())
	}

	if tableUpdateDesc.TableDescription.ProvisionedThroughput.ReadCapacityUnits != 10 {
		t.Fatalf("", tableUpdateDesc.TableDescription.ProvisionedThroughput.ReadCapacityUnits)
	}

	if tableUpdateDesc.TableDescription.ProvisionedThroughput.NumberOfDecreasesToday != 1 {
		t.Fatalf("", tableUpdateDesc.TableDescription.ProvisionedThroughput.NumberOfDecreasesToday)
	}

	reqB = &UpdateTableRequest{
		ProvisionedThroughput: ProvisionedThroughput{ReadCapacityUnits: 15, WriteCapacityUnits: 10},
		TableName:             "bar",
	}
	tableUpdateDesc, err = table.UpdateTable(reqB)
	if err != nil {
		t.Fatalf(err.Error())
	}

	if tableUpdateDesc.TableDescription.ProvisionedThroughput.ReadCapacityUnits != 15 {
		t.Fatalf("", tableUpdateDesc.TableDescription.ProvisionedThroughput.ReadCapacityUnits)
	}

	if tableUpdateDesc.TableDescription.ProvisionedThroughput.NumberOfDecreasesToday != 2 {
		t.Fatalf("", tableUpdateDesc.TableDescription.ProvisionedThroughput.NumberOfDecreasesToday)
	}
}

// type QuerySelect string

// const (
// 	AllAttributesQuerySelect                QuerySelect = "ALL_ATTRIBUTES"
// 	AllProjectedAttributesQuerySelect                   = "ALL_PROJECTED_ATTRIBUTES"
// 	SpecificAttributesAttributesQuerySelect             = "SPECIFIC_ATTRIBUTES"
// 	CountQuerySelect                                    = "COUNT"
// )

// type QueryRequest struct {
// 	AttributesToGet        []string
// 	ConsistentRead         bool
// 	ExclusiveStartKey      map[string]AttributeValue // min 3 max 255
// 	TableName              string
// 	IndexName              string
// 	KeyConditions          map[string]Condition
// 	Limit                  int
// 	ReturnConsumedCapacity ReturnConsumedCapacity
// 	Select                 QuerySelect
// }

// type QueryResult struct {
// 	ConsumedCapacity ConsumedCapacity
// 	Count            int
// 	Items            []map[string]AttributeValue
// 	LastEvaluatedKey map[string]AttributeValue
// }

func TestQuery(t *testing.T) {
	db := NewDB()
	CreateTable(db, "bax")
	table := db.GetTable("bax")
	InsertItem(table, "bax", map[string]AttributeValue{"id": AttributeValue{S: "bar"}, "foo": AttributeValue{S: "bam"}})
	InsertItem(table, "bax", map[string]AttributeValue{"id": AttributeValue{S: "ba"}, "foo": AttributeValue{S: "bar"}})
	InsertItem(table, "bax", map[string]AttributeValue{"id": AttributeValue{S: "bat"}, "foo": AttributeValue{S: "bar2"}})
	InsertItem(table, "bax", map[string]AttributeValue{"id": AttributeValue{S: "at"}, "foo": AttributeValue{S: "bar3"}})
	InsertItem(table, "bax", map[string]AttributeValue{"id": AttributeValue{S: "1"}, "foo": AttributeValue{S: "bar4"}})
	InsertItem(table, "bax", map[string]AttributeValue{"id": AttributeValue{S: "2"}, "foo": AttributeValue{S: "bar5"}})
	InsertItem(table, "bax", map[string]AttributeValue{"id": AttributeValue{S: "3"}, "foo": AttributeValue{S: "bar6"}})
	InsertItem(table, "bax", map[string]AttributeValue{"id": AttributeValue{S: "4"}, "foo": AttributeValue{S: "bar7"}})
	InsertItem(table, "bax", map[string]AttributeValue{"id": AttributeValue{S: "5"}, "foo": AttributeValue{S: "bar8"}})

	// Get one specific item

	req := &QueryRequest{
		KeyConditions:   map[string]Condition{"id": Condition{EQ, []AttributeValue{AttributeValue{S: "ba"}}}},
		TableName:       "bax",
		AttributesToGet: []string{"id", "foo"},
	}

	result, _ := table.Query(req)
	if result == nil {
		t.Fail()
	}

	if result.Items[0]["foo"].S != "bar" || len(result.Items) != 1 {
		t.Fatalf("%+v", result)
	}

	// GT

	req = &QueryRequest{
		KeyConditions:   map[string]Condition{"id": Condition{GT, []AttributeValue{AttributeValue{S: "ba"}}}},
		TableName:       "bax",
		AttributesToGet: []string{"id", "foo"},
	}

	result, _ = table.Query(req)
	if result == nil {
		t.Fail()
	}

	if result.Items[0]["foo"].S != "bam" || len(result.Items) != 2 {
		t.Fatalf("%+v", result)
	}

	// GE

	req = &QueryRequest{
		KeyConditions:   map[string]Condition{"id": Condition{GE, []AttributeValue{AttributeValue{S: "ba"}}}},
		TableName:       "bax",
		AttributesToGet: []string{"id", "foo"},
	}

	result, _ = table.Query(req)
	if result == nil {
		t.Fail()
	}

	if result.Items[1]["foo"].S != "bar" || len(result.Items) != 3 {
		t.Fatalf("%+v", result)
	}

	// LT

	req = &QueryRequest{
		KeyConditions:   map[string]Condition{"id": Condition{LT, []AttributeValue{AttributeValue{S: "bar"}}}},
		TableName:       "bax",
		AttributesToGet: []string{"id", "foo"},
	}

	result, _ = table.Query(req)
	if result == nil {
		t.Fail()
	}

	if result.Items[0]["foo"].S != "bar" || len(result.Items) != 7 {
		t.Fatalf("%+v", len(result.Items), result)
	}

	// LE

	req = &QueryRequest{
		KeyConditions:   map[string]Condition{"id": Condition{LE, []AttributeValue{AttributeValue{S: "bar"}}}},
		TableName:       "bax",
		AttributesToGet: []string{"id", "foo"},
	}

	result, _ = table.Query(req)
	if result == nil {
		t.Fail()
	}

	if result.Items[0]["foo"].S != "bam" || len(result.Items) != 8 {
		t.Fatalf("%+v", len(result.Items), result)
	}

	// NE

	req = &QueryRequest{
		KeyConditions:   map[string]Condition{"id": Condition{NE, []AttributeValue{AttributeValue{S: "bar"}}}},
		TableName:       "bax",
		AttributesToGet: []string{"id", "foo"},
	}

	result, _ = table.Query(req)
	if result == nil {
		t.Fail()
	}

	if result.Items[0]["foo"].S != "bar" || len(result.Items) != 8 {
		t.Fatalf("%+v", len(result.Items), result)
	}

	// IN

	req = &QueryRequest{
		KeyConditions:   map[string]Condition{"id": Condition{IN, []AttributeValue{AttributeValue{S: "bar"}, AttributeValue{S: "ba"}}}},
		TableName:       "bax",
		AttributesToGet: []string{"id", "foo"},
	}

	result, _ = table.Query(req)
	if result == nil {
		t.Fail()
	}

	if result.Items[0]["foo"].S != "bam" || len(result.Items) != 2 {
		t.Fatalf("%+v", len(result.Items), result)
	}

	// BETWEEN

	req = &QueryRequest{
		KeyConditions:   map[string]Condition{"id": Condition{BETWEEN, []AttributeValue{AttributeValue{S: "2"}, AttributeValue{S: "4"}}}},
		TableName:       "bax",
		AttributesToGet: []string{"id", "foo"},
	}

	result, _ = table.Query(req)
	if result == nil {
		t.Fail()
	}

	if result.Items[0]["foo"].S != "bar5" || len(result.Items) != 3 {
		t.Fatalf("%+v", len(result.Items), result)
	}

	// BEGINS_WITH

	req = &QueryRequest{
		KeyConditions:   map[string]Condition{"id": Condition{BEGINS_WITH, []AttributeValue{AttributeValue{S: "ba"}}}},
		TableName:       "bax",
		AttributesToGet: []string{"id", "foo"},
	}

	result, _ = table.Query(req)
	if result == nil {
		t.Fail()
	}

	if result.Items[0]["foo"].S != "bam" || len(result.Items) != 3 {
		t.Fatalf("%+v", len(result.Items), result)
	}
}


func TestQueryLimit(t *testing.T) {
	db := NewDB()
	CreateTable(db, "bax")
	table := db.GetTable("bax")
	InsertItem(table, "bax", map[string]AttributeValue{"id": AttributeValue{S: "bar"}, "foo": AttributeValue{S: "bam"}})
	InsertItem(table, "bax", map[string]AttributeValue{"id": AttributeValue{S: "ba"}, "foo": AttributeValue{S: "bar"}})
	InsertItem(table, "bax", map[string]AttributeValue{"id": AttributeValue{S: "bat"}, "foo": AttributeValue{S: "bar2"}})
	InsertItem(table, "bax", map[string]AttributeValue{"id": AttributeValue{S: "at"}, "foo": AttributeValue{S: "bar3"}})
	InsertItem(table, "bax", map[string]AttributeValue{"id": AttributeValue{S: "1"}, "foo": AttributeValue{S: "bar4"}})
	InsertItem(table, "bax", map[string]AttributeValue{"id": AttributeValue{S: "2"}, "foo": AttributeValue{S: "bar5"}})
	InsertItem(table, "bax", map[string]AttributeValue{"id": AttributeValue{S: "3"}, "foo": AttributeValue{S: "bar6"}})
	InsertItem(table, "bax", map[string]AttributeValue{"id": AttributeValue{S: "4"}, "foo": AttributeValue{S: "bar7"}})
	InsertItem(table, "bax", map[string]AttributeValue{"id": AttributeValue{S: "5"}, "foo": AttributeValue{S: "bar8"}})
	
}
// type ConditionOperator string

// const (
// 	EQ           ConditionOperator = "EQ"
// 	NE                             = "NE"
// 	IN                             = "IN"
// 	LE                             = "LE"
// 	LT                             = "LT"
// 	GE                             = "GE"
// 	GT                             = "GT"
// 	BETWEEN                        = "BETWEEN"
// 	NOT_NULL                       = "NOT_NULL"
// 	NULL                           = "NULL"
// 	CONTAINS                       = "CONTAINS"
// 	NOT_CONTAINS                   = "NOT_CONTAINS"
// 	BEGINS_WITH                    = "BEGINS_WITH"
// )

// type Condition struct {
// 	ConditionOperator  ConditionOperator
// 	AttributeValueList []AttributeValue
// }

//
// Helpers
//

func InsertItem(table *Table, tableName string, item map[string]AttributeValue) {
	req := &PutItemRequest{
		Item:         item,
		TableName:    tableName,
		ReturnValues: AllNewReturnValues,
	}

	_, err := table.PutItem(req)
	if err != nil {
		panic(err)
	}
	// return result.Ite
}
