package dynamockdb

import (
	"time"
)

type AttributeType string

const (
	StringAttributeType    AttributeType = "S"
	StringSetAttributeType               = "SS"
	NumberAttributeType                  = "N"
	NumberSetAttributeType               = "NS"
	BinaryAttributeType                  = "B"
	BinarySetAttributeType               = "BS"
)

type AttributeDefinition struct {
	AttributeName string // min 1 max 255
	AttributeType AttributeType
}

type UpdateAction string

const (
	PutUpdateAction    UpdateAction = "PUT"
	DeleteUpdateAction              = "DELETE"
	AddUpdateAction                 = "ADD"
)

// Attribute values cannot be null; string and binary type attributes must have
// lengths greater than zero; and set type attributes must not be empty.
// Requests with empty values will be rejected with a ValidationException.
type AttributeValueUpdate struct {
	Action UpdateAction
	Value  AttributeValue
}

type BatchGetItemResult struct {
	ConsumedCapacity []ConsumedCapacity
	Responses        map[string]*KeysAndAttributes
	UnprocessedKeys  map[string]*KeysAndAttributes
}

type BatchWriteItemResult struct {
	ConsumedCapacity      []ConsumedCapacity
	ItemCollectionMetrics map[string]ItemCollectionMetrics
	UnprocessedItems      map[string]WriteRequest
}

type ConditionOperator string

const (
	EQ           ConditionOperator = "EQ"
	NE                             = "NE"
	IN                             = "IN"
	LE                             = "LE"
	LT                             = "LT"
	GE                             = "GE"
	GT                             = "GT"
	BETWEEN                        = "BETWEEN"
	NOT_NULL                       = "NOT_NULL"
	NULL                           = "NULL"
	CONTAINS                       = "CONTAINS"
	NOT_CONTAINS                   = "NOT_CONTAINS"
	BEGINS_WITH                    = "BEGINS_WITH"
)

type Condition struct {
	ConditionOperator  ConditionOperator
	AttributeValueList []AttributeValue
}

type ConsumedCapacity struct {
	CapacityUnits float64
	TableName     string // min 3 max 255
}

type CreateTableRequest struct {
	AttributeDefinitions  []AttributeDefinition
	KeySchema             []KeySchemaElement
	ProvisionedThroughput ProvisionedThroughput
	TableName             string // min 3 max 255
	LocalSecondaryIndexes []LocalSecondaryIndex
}

type CreateTableResult struct {
	TableDescription TableDescription
}

type DeleteItemRequest struct {
	Expected                    map[string]ExpectedAttributeValue
	Key                         map[string]AttributeValue
	TableName                   string
	ReturnConsumedCapacity      ReturnConsumedCapacity
	ReturnItemCollectionMetrics ReturnItemCollectionMetrics
	ReturnValues                ReturnValues
}

type DeleteItemResult struct {
	Attributes            map[string]AttributeValue
	ConsumedCapacity      ConsumedCapacity
	ItemCollectionMetrics ItemCollectionMetrics
}

type DeleteRequest struct {
	Key map[string]AttributeValue
}

type DeleteTableRequest struct {
	TableName string
}

type DeleteTableResult struct {
	TableDescription TableDescription
}

type DescribeTableRequest struct {
	TableName string
}
type DescribeTableResult struct {
	Table TableDescription
}

type ExpectedAttributeValue struct {
	Exists bool
	Value  AttributeValue
}

type GetItemRequest struct {
	Key                    map[string]AttributeValue
	TableName              string
	AttributesToGet        []string
	ConsistentRead         bool
	ReturnConsumedCapacity ReturnConsumedCapacity
}

type GetItemResult struct {
	ConsumedCapacity ConsumedCapacity
	Item             map[string]AttributeValue
}

type ItemCollectionMetrics struct {
	ItemCollectionKey   map[string]AttributeValue
	SizeEstimateRangeGB []float64
}

type KeyType string

const (
	HashKeyType  KeyType = "HASH"
	RangeKeyType         = "RANGE"
)

type KeySchemaElement struct {
	AttributeName string // min 1 max 255
	KeyType       KeyType
}

type KeysAndAttributes struct {
	Keys            []string
	AttributesToGet []string
	ConsistentRead  bool
}

type ListTablesRequest struct {
	ExclusiveStartTableName string // min 3 max 255
	Limit                   int
}

type ListTablesResult struct {
	LastEvaluatedTableName string `json:",omitempty"`
	TableNames             []string
}

type LocalSecondaryIndex struct {
	IndexName  string // min 3 max 255
	KeySchema  KeySchemaElement
	Projection Projection
}

type LocalSecondaryIndexDescription struct {
	IndexName      string // min 3 max 255
	IndexSizeBytes float32
	ItemCount      float32
	KeySchema      []KeySchemaElement
	Projection     Projection
}

type ProjectionType string

const (
	KeysOnlyProjectionType ProjectionType = "KEYS_ONLY"
	IncludeProjectionType                 = "INCLUDE"
	AllProjectionType                     = "ALL"
)

type Projection struct {
	NonKeyAttributes []string // min 1 item in list max 20
	ProjectionType   ProjectionType
}

type ProvisionedThroughput struct {
	ReadCapacityUnits  float32
	WriteCapacityUnits float32
}

type ProvisionedThroughputDescription struct {
	LastDecreaseDateTime   time.Time
	LastIncreaseDateTime   time.Time
	NumberOfDecreasesToday float32
	numberOfDecreasesDay   time.Time
	ReadCapacityUnits      float32
	WriteCapacityUnits     float32
}

type ReturnConsumedCapacity string

const (
	TotalReturnConsumedCapacity ReturnConsumedCapacity = "TOTAL"
	NoneReturnConsumedCapacity                         = "NONE"
)

type ReturnItemCollectionMetrics string

const (
	SizeReturnItemCollectionMetrics ReturnItemCollectionMetrics = "SIZE"
	NoneReturnItemCollectionMetrics                             = "NONE"
)

type ReturnValues string

const (
	NoneReturnValues       ReturnValues = "NONE"
	AllOldReturnValues                  = "ALL_OLD"
	UpdatedOldReturnValues              = "UPDATED_OLD"
	AllNewReturnValues                  = "ALL_NEW"
	UpdatedNewReturnValues              = "UPDATED_NEW"
)

type PutItemRequest struct {
	Item                        map[string]AttributeValue
	TableName                   string
	Expected                    map[string]ExpectedAttributeValue
	ReturnConsumedCapacity      ReturnConsumedCapacity
	ReturnItemCollectionMetrics ReturnItemCollectionMetrics
	ReturnValues                ReturnValues
}

type PutItemResult struct {
	Attributes            map[string]AttributeValue
	ConsumedCapacity      ConsumedCapacity
	ItemCollectionMetrics ItemCollectionMetrics
}

type PutRequest struct {
	Item map[string]AttributeValue
}

type QuerySelect string

const (
	AllAttributesQuerySelect                QuerySelect = "ALL_ATTRIBUTES"
	AllProjectedAttributesQuerySelect                   = "ALL_PROJECTED_ATTRIBUTES"
	SpecificAttributesAttributesQuerySelect             = "SPECIFIC_ATTRIBUTES"
	CountQuerySelect                                    = "COUNT"
)

type QueryRequest struct {
	AttributesToGet        []string
	ConsistentRead         bool
	ExclusiveStartKey      map[string]AttributeValue // min 3 max 255
	TableName              string
	IndexName              string
	KeyConditions          map[string]Condition
	Limit                  int
	ReturnConsumedCapacity ReturnConsumedCapacity
	Select                 QuerySelect
}

type QueryResult struct {
	ConsumedCapacity ConsumedCapacity
	Count            int
	Items            []map[string]AttributeValue
	LastEvaluatedKey map[string]AttributeValue
}

type ScanResult struct {
	ConsumedCapacity ConsumedCapacity
	Count            int
	Items            []string
	LastEvaluatedKey map[string]AttributeValue
	ScannedCount     int
}

type TableStatus string

const (
	CreatingTableStatus TableStatus = "CREATING"
	UpdatingTableStatus             = "UPDATING"
	DeletingTableStatus             = "DELETING"
	ActiveTableStatus               = "ACTIVE"
)

type TableDescription struct {
	AttributeDefinitions  []AttributeDefinition
	CreationDateTime      time.Time
	ItemCount             float32
	KeySchema             []KeySchemaElement
	LocalSecondaryIndexes []LocalSecondaryIndex
	ProvisionedThroughput ProvisionedThroughputDescription
	TableName             string // min 3 max 255
	TableSizeBytes        float32
	TableStatus           TableStatus
}

type UpdateItemRequest struct {
	AttributeUpdates            map[string]AttributeValueUpdate
	TableName                   string
	Expected                    map[string]ExpectedAttributeValue
	Key                         map[string]AttributeValue
	ReturnConsumedCapacity      ReturnConsumedCapacity
	ReturnItemCollectionMetrics ReturnItemCollectionMetrics
	ReturnValues                ReturnValues
}

type UpdateItemResult struct {
	Attributes            map[string]AttributeValue
	ConsumedCapacity      ConsumedCapacity
	ItemCollectionMetrics ItemCollectionMetrics
}

type UpdateTableRequest struct {
	TableName             string // min 3 max 255
	ProvisionedThroughput ProvisionedThroughput
}

type UpdateTableResult struct {
	TableDescription TableDescription
}

type WriteRequest struct {
	DeleteRequest DeleteRequest
	PutRequest    PutRequest
}
