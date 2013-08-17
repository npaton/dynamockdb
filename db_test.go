package dynamockdb

import (
	"testing"
)

func TestCreateTable(t *testing.T) {
	db := NewDB()
	req := &CreateTableRequest{
		AttributeDefinitions:  []AttributeDefinition{AttributeDefinition{AttributeName: "foo", AttributeType: StringAttributeType}},
		KeySchema:             []KeySchemaElement{KeySchemaElement{AttributeName: "id", KeyType: HashKeyType}},
		ProvisionedThroughput: ProvisionedThroughput{ReadCapacityUnits: 5, WriteCapacityUnits: 5},
		TableName:             "bar",
		// LocalSecondaryIndexes: []LocalSecondaryIndex{LocalSecondaryIndex{IndexName:"fooIndex", KeySchema: KeySchemaElement{AttributeName: "foo"}, Projection: Projection{}  }}
	}
	tableDesc := db.CreateTable(req)

	if tableDesc.TableDescription.AttributeDefinitions[0].AttributeName != "foo" {
		t.Fail()
	}

	// ! Testing internals
	if db.Tables["bar"] == nil {
		t.Fail()
	}
}

func TestDescribeTable(t *testing.T) {
	db := NewDB()
	CreateTable(db, "bar")
	result := db.DescribeTable(&DescribeTableRequest{"bar"})
	if result.Table.AttributeDefinitions[0].AttributeName != "foo" {
		t.Fail()
	}
}

func TestListTables(t *testing.T) {
	db := NewDB()
	CreateTable(db, "bar")
	CreateTable(db, "baz")
	CreateTable(db, "boz")
	CreateTable(db, "bor")
	CreateTable(db, "bez")

	result := db.ListTables(&ListTablesRequest{})

	ExpectTableNames(t, []string{"bar", "baz", "boz", "bor", "bez"}, result.TableNames)

	expectedCount, actualCount := 2, 0
	resultA := db.ListTables(&ListTablesRequest{Limit: expectedCount})
	expectedB := make([]string, 0, 3)
	for _, tableName := range []string{"bar", "baz", "boz", "bor", "bez"} {
		found := false
		for _, tableNameA := range resultA.TableNames {
			if tableNameA == tableName {
				found = true
				break
			}
		}
		if !found {
			expectedB = append(expectedB, tableName)
		} else {
			actualCount++
		}
	}

	if expectedCount != actualCount {
		t.Fatalf("Expected %d Tables returned, got %d", expectedCount, actualCount)
	}

	resultB := db.ListTables(&ListTablesRequest{ExclusiveStartTableName: resultA.LastEvaluatedTableName})

	ExpectTableNames(t, expectedB, resultB.TableNames)
}

func TestDeleteTable(t *testing.T) {
	db := NewDB()
	CreateTable(db, "bar")
	CreateTable(db, "baz")
	CreateTable(db, "boz")
	CreateTable(db, "bor")
	CreateTable(db, "bez")

	tableDesc, err := db.DeleteTable(&DeleteTableRequest{"baz"})
	if err != nil {
		t.Fatalf(err.Error())
	}
	if tableDesc.TableDescription.TableName != "baz" {
		t.Fatalf("", tableDesc.TableDescription.TableName)
	}

	result := db.ListTables(&ListTablesRequest{})
	ExpectTableNames(t, []string{"bar", "boz", "bor", "bez"}, result.TableNames)
	DontExpectTableName(t, result.TableNames, "baz")
}

//
//  Helper functions
//

func CreateTable(db *DB, tableName string) *CreateTableResult {
	req := &CreateTableRequest{
		AttributeDefinitions:  []AttributeDefinition{AttributeDefinition{AttributeName: "foo", AttributeType: StringAttributeType}, AttributeDefinition{AttributeName: "id", AttributeType: StringAttributeType}},
		KeySchema:             []KeySchemaElement{KeySchemaElement{AttributeName: "id", KeyType: HashKeyType}},
		ProvisionedThroughput: ProvisionedThroughput{ReadCapacityUnits: 5, WriteCapacityUnits: 5},
		TableName:             tableName,
	}
	return db.CreateTable(req)
}

func ExpectTableNames(t *testing.T, expectedTableNames, otherTableNames []string) {
	for _, name := range expectedTableNames {
		found := false
		for _, otherName := range otherTableNames {
			if name == otherName {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("table not found %s", name, expectedTableNames, otherTableNames)
		}
	}
}

func DontExpectTableName(t *testing.T, tableNames []string, name string) {
	found := false
	for _, otherName := range tableNames {
		if name == otherName {
			found = true
			break
		}
	}
	if found {
		t.Fatalf("table found %s, shouldn't be there", name)
	}
}
