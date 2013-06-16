package main

import (
	"fmt"
)

type DB struct {
	Tables map[string]*Table
}

func NewDB() *DB {
	return &DB{Tables: make(map[string]*Table)}
}

func (db *DB) GetTable(tableName string) *Table {
	return db.Tables[tableName]
}

func (db *DB) CreateTable(req *CreateTableRequest) *CreateTableResult {
	db.Tables[req.TableName] = NewTable(req)
	return &CreateTableResult{db.Tables[req.TableName].TableDescription}
}

func (db *DB) DescribeTable(req *DescribeTableRequest) *DescribeTableResult {
	return &DescribeTableResult{db.Tables[req.TableName].TableDescription}
}

func (db *DB) ListTables(req *ListTablesRequest) ListTablesResult {
	tableNames := make([]string, len(db.Tables))
	lastTableName := ""
	count := 0
	passedStartTableName := false
	for tableName, _ := range db.Tables {
		if req.ExclusiveStartTableName != "" {
			if passedStartTableName {
				tableNames = append(tableNames, tableName)
			} else {
				if tableName == req.ExclusiveStartTableName {
					passedStartTableName = true
				}
			}
		} else {
			tableNames = append(tableNames, tableName)
		}
		lastTableName = tableName
		count += 1
		if req.Limit >= 0 && req.Limit == count {
			break
		}
	}
	return ListTablesResult{TableNames: tableNames, LastEvaluatedTableName: lastTableName}
}

func (db *DB) DeleteTable(req *DeleteTableRequest) (*DeleteTableResult, error) {
	if _, found := db.Tables[req.TableName]; !found {
		return nil, fmt.Errorf("", db.Tables, req.TableName)
	}
	tableDesc := db.Tables[req.TableName].TableDescription
	delete(db.Tables, req.TableName)
	return &DeleteTableResult{tableDesc}, nil
}
