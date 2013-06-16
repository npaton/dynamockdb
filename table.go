package main

import (
	"fmt"
	// "strconv"
	"strings"
	"time"
)

type Table struct {
	TableDescription TableDescription
	Items            map[string]map[string]AttributeValue
	InsertOrder      []string // Used for scanning
	ConsumedCapacity ConsumedCapacity
}

func NewTable(req *CreateTableRequest) *Table {
	desc := TableDescription{
		AttributeDefinitions:  req.AttributeDefinitions,
		CreationDateTime:      time.Now(),
		KeySchema:             req.KeySchema,
		LocalSecondaryIndexes: req.LocalSecondaryIndexes,
		ProvisionedThroughput: ProvisionedThroughputDescription{
			LastIncreaseDateTime:   time.Now(),
			NumberOfDecreasesToday: 0,
			ReadCapacityUnits:      req.ProvisionedThroughput.ReadCapacityUnits,
			WriteCapacityUnits:     req.ProvisionedThroughput.WriteCapacityUnits,
		},
		TableName:   req.TableName,
		TableStatus: ActiveTableStatus,
	}

	return &Table{
		TableDescription: desc,
		Items:            make(map[string]map[string]AttributeValue),
		InsertOrder:      make([]string, 0),
	}
}

func (t *Table) UpdateTable(req *UpdateTableRequest) (*UpdateTableResult, error) {
	decreased := false

	if t.TableDescription.ProvisionedThroughput.ReadCapacityUnits < req.ProvisionedThroughput.ReadCapacityUnits {
		t.TableDescription.ProvisionedThroughput.LastIncreaseDateTime = time.Now()
	} else {
		decreased = true
		t.TableDescription.ProvisionedThroughput.LastDecreaseDateTime = time.Now()
	}

	if t.TableDescription.ProvisionedThroughput.WriteCapacityUnits < req.ProvisionedThroughput.WriteCapacityUnits {
		t.TableDescription.ProvisionedThroughput.LastIncreaseDateTime = time.Now()
	} else {
		decreased = true
		t.TableDescription.ProvisionedThroughput.LastDecreaseDateTime = time.Now()
	}

	y, m, d := time.Now().Date()
	today := time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
	var compTime time.Time
	if t.TableDescription.ProvisionedThroughput.numberOfDecreasesDay == compTime {
		t.TableDescription.ProvisionedThroughput.numberOfDecreasesDay = today
	}

	if t.TableDescription.ProvisionedThroughput.numberOfDecreasesDay != today {
		t.TableDescription.ProvisionedThroughput.numberOfDecreasesDay = today
		t.TableDescription.ProvisionedThroughput.NumberOfDecreasesToday = 0
	}

	if decreased {
		t.TableDescription.ProvisionedThroughput.NumberOfDecreasesToday += 1
	}

	t.TableDescription.ProvisionedThroughput.ReadCapacityUnits = req.ProvisionedThroughput.ReadCapacityUnits
	t.TableDescription.ProvisionedThroughput.WriteCapacityUnits = req.ProvisionedThroughput.WriteCapacityUnits

	result := &UpdateTableResult{
		TableDescription: t.TableDescription,
	}

	return result, nil
}

func (t *Table) HashKey() *AttributeDefinition {
	for _, el := range t.TableDescription.KeySchema {
		if el.KeyType == HashKeyType {
			return t.GetAttribute(el.AttributeName)
		}
	}
	return nil
}

func (t *Table) RangeKey() *AttributeDefinition {
	for _, el := range t.TableDescription.KeySchema {
		if el.KeyType == RangeKeyType {
			return t.GetAttribute(el.AttributeName)
		}
	}
	return nil
}

func (t *Table) GetAttribute(name string) *AttributeDefinition {
	for _, def := range t.TableDescription.AttributeDefinitions {
		if def.AttributeName == name {
			return &def
		}
	}
	return nil
}

func (t *Table) UpdateItem(req *UpdateItemRequest) (*UpdateItemResult, error) {
	hashKey := t.HashKey()
	key := ""
	returnItem := make(map[string]AttributeValue)

	if val, ok := req.Key[hashKey.AttributeName]; ok {
		key = val.Value(hashKey.AttributeType)
	} else {
		return nil, fmt.Errorf("PuItem: Missing HashKey. %s", t.HashKey().AttributeName)
	}

	if t.Items[key] == nil {
		return nil, fmt.Errorf("UpdateItem: Item not found")
	} else {
		if req.ReturnValues == AllOldReturnValues || req.ReturnValues == UpdatedOldReturnValues {
			for k, v := range t.Items[key] {
				returnItem[k] = v
			}
		}
	}

	// Validate expections are met
	err := t.validateExpectations(req.Expected, key)
	if err != nil {
		return nil, err
	}

	for k, v := range req.AttributeUpdates {
		switch v.Action {
		case PutUpdateAction:
			t.Items[key][k] = v.Value
		case DeleteUpdateAction:
			delete(t.Items[key], k)
		case AddUpdateAction:
			attr := t.GetAttribute(k)
			switch attr.AttributeType {
			case NumberAttributeType:
				if v.Value.Value(attr.AttributeType) == "" {
					return nil, fmt.Errorf("UpdateItem: ADD to int field with non int value")
				}
				attrVal := t.Items[key][k]
				attrVal.N = attrVal.N + v.Value.N
			}

		}
	}

	if req.ReturnValues == AllNewReturnValues || req.ReturnValues == UpdatedNewReturnValues {
		returnItem = t.Items[key]
	}

	if req.ReturnValues == UpdatedOldReturnValues || req.ReturnValues == UpdatedNewReturnValues {
		newItem := make(map[string]AttributeValue)
		for k, _ := range req.AttributeUpdates {
			newItem[k] = returnItem[k]
		}
		returnItem = newItem
	}

	result := &UpdateItemResult{
		Attributes: returnItem,
	}

	return result, nil
}

func (t *Table) PutItem(req *PutItemRequest) (*PutItemResult, error) {

	// Hash key for Table
	hashKey := t.HashKey()

	// Hash key value for item
	key := ""

	// Item that will be returned at the end
	returnItem := make(map[string]AttributeValue)

	// Verify the new item contain the Hash key
	if val, ok := req.Item[hashKey.AttributeName]; ok {
		key = val.Value(hashKey.AttributeType)
	} else {
		return nil, fmt.Errorf("PuItem: Missing HashKey. %s", t.HashKey().AttributeName)
	}

	// If no item there insert it
	if t.Items[key] == nil {
		t.Items[key] = make(map[string]AttributeValue)
		t.InsertOrder = append(t.InsertOrder, key)
	} else {
		// Otherwise copy old values if needed in return item
		if req.ReturnValues == AllOldReturnValues || req.ReturnValues == UpdatedOldReturnValues {
			for k, v := range t.Items[key] {
				returnItem[k] = v
			}
		}
	}

	err := t.validateExpectations(req.Expected, key)
	if err != nil {
		return nil, err
	}

	// Replace item
	t.Items[key] = req.Item

	// If return value new demanded replace returnItem
	if req.ReturnValues == AllNewReturnValues || req.ReturnValues == UpdatedNewReturnValues {
		returnItem = t.Items[key]
	}

	// If updated values wanted replace returnItem again
	if req.ReturnValues == UpdatedOldReturnValues || req.ReturnValues == UpdatedNewReturnValues {
		newItem := make(map[string]AttributeValue)
		for k, _ := range req.Item {
			newItem[k] = returnItem[k]
		}
		returnItem = newItem
	}

	result := &PutItemResult{
		Attributes: returnItem,
	}

	return result, nil
}

func (t *Table) DeleteItem(req *DeleteItemRequest) (*DeleteItemResult, error) {

	if _, ok := req.Key[t.HashKey().AttributeName]; !ok {
		return nil, fmt.Errorf("GetItem: Invalid key %v", req.Key)
	}

	val := req.Key[t.HashKey().AttributeName]
	key := val.Value(t.HashKey().AttributeType)

	if _, ok := t.Items[key]; !ok {
		return nil, fmt.Errorf("GetItem: Not found for key '%v'", t.HashKey().AttributeName)
	}

	err := t.validateExpectations(req.Expected, key)
	if err != nil {
		return nil, err
	}

	returnItem := t.Items[key]

	delete(t.Items, key)

	newInsertOrder := make([]string, 0, len(t.InsertOrder)-1)
	for _, v := range t.InsertOrder {
		if v == key {
			newInsertOrder = append(newInsertOrder, v)
		}
	}
	t.InsertOrder = newInsertOrder

	result := &DeleteItemResult{
		Attributes: returnItem,
	}

	return result, nil
}

func (t *Table) validateExpectations(expected map[string]ExpectedAttributeValue, key string) error {
	for field, exp := range expected {
		val, exists := t.Items[key][field]
		if exists != exp.Exists {
			return fmt.Errorf("PuItem: Expectation not met: %v", exp)
		}
		field := t.GetAttribute(field)
		err := val.ValidateExpectations(field.AttributeType, exp)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) GetItem(req *GetItemRequest) (*GetItemResult, error) {
	if _, ok := req.Key[t.HashKey().AttributeName]; !ok {
		return nil, fmt.Errorf("GetItem: Invalid key %v", req.Key)
	}

	val := req.Key[t.HashKey().AttributeName]
	key := val.Value(t.HashKey().AttributeType)

	if _, ok := t.Items[key]; !ok {
		return nil, fmt.Errorf("GetItem: Not found for key '%v'", t.HashKey().AttributeName)
	}

	item := t.Items[key]

	returnItem := make(map[string]AttributeValue)
	if len(req.AttributesToGet) > 0 {
		for _, attr := range req.AttributesToGet {
			if _, ok := item[attr]; ok {
				returnItem[attr] = item[attr]
			}
		}
	} else {
		for attr := range item {
			returnItem[attr] = item[attr]
		}
	}
	result := &GetItemResult{
		Item: returnItem,
	}

	if req.ReturnConsumedCapacity == TotalReturnConsumedCapacity {
		result.ConsumedCapacity = t.ConsumedCapacity
	}

	return result, nil
}

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
// 	Items            []string
// 	LastEvaluatedKey map[string]AttributeValue
// }

func (t *Table) Query(req *QueryRequest) (*QueryResult, error) {
	count := 0
	items := make([]map[string]AttributeValue, 0, 20)
	hashKey := t.HashKey()
	rangeKey := t.RangeKey()
	var hashCondition Condition
	var rangeCondition Condition
	
	for keyName, condition := range req.KeyConditions {
		if keyName == hashKey.AttributeName {
			hashCondition = condition
		} else if keyName == rangeKey.AttributeName {
			rangeCondition = condition
		} else {
			return nil, fmt.Errorf("Query: Invalid KeyCondition, not on has or range fields. %v", keyName)
		}
	}
	
	for _, key := range t.InsertOrder {
		value := t.Items[key]
		switch hashCondition.ConditionOperator {
		case EQ:
			if key == hashCondition.AttributeValueList[0].Value(hashKey.AttributeType) {
				items = append(items, value)
			}
		case LE:
			if hashCondition.AttributeValueList[0].Value(hashKey.AttributeType) >= key {
				items = append(items, value)
			}
		case LT:
			if hashCondition.AttributeValueList[0].Value(hashKey.AttributeType) > key {
				items = append(items, value)
			}
		case GE:
			if hashCondition.AttributeValueList[0].Value(hashKey.AttributeType) <= key {
				items = append(items, value)
			}
		case GT:
			if hashCondition.AttributeValueList[0].Value(hashKey.AttributeType) < key {
				items = append(items, value)
			}
		case NE:
			if hashCondition.AttributeValueList[0].Value(hashKey.AttributeType) != key {
				items = append(items, value)
			}
		case IN:
			for _, attrib := range hashCondition.AttributeValueList {
				if attrib.Value(hashKey.AttributeType) == key {
					items = append(items, value)
					break
				}
			}
		case BEGINS_WITH:
			if strings.HasPrefix(key, hashCondition.AttributeValueList[0].Value(hashKey.AttributeType)) {
				items = append(items, value)
			}
		case BETWEEN:
			if hashCondition.AttributeValueList[0].Value(hashKey.AttributeType) <= key && hashCondition.AttributeValueList[1].Value(hashKey.AttributeType) >= key {
				items = append(items, value)
			}
		}
	}

	newItems := make([]map[string]AttributeValue, 0, len(items))
	if rangeKey != nil {
		for _, value := range items {
			fmt.Println("value", value, rangeKey.AttributeName)
			v1 := value[rangeKey.AttributeName]
			v2 := rangeCondition.AttributeValueList[0]
			switch rangeCondition.ConditionOperator {
			case EQ:
				if v1.Value(rangeKey.AttributeType) == v2.Value(rangeKey.AttributeType) {
					newItems = append(newItems, value)
				}
			case LE:
				if v1.Value(rangeKey.AttributeType) <= v2.Value(rangeKey.AttributeType) {
					newItems = append(newItems, value)
				}
			case LT:
				if v1.Value(rangeKey.AttributeType) < v2.Value(rangeKey.AttributeType) {
					newItems = append(newItems, value)
				}
			case GE:
				if v1.Value(rangeKey.AttributeType) >= v2.Value(rangeKey.AttributeType) {
					newItems = append(newItems, value)
				}
			case GT:
				if v1.Value(rangeKey.AttributeType) > v2.Value(rangeKey.AttributeType) {
					newItems = append(newItems, value)
				}
			case NE:
				if v1.Value(rangeKey.AttributeType) != v2.Value(rangeKey.AttributeType) {
					newItems = append(newItems, value)
				}
			case IN:
				for _, attrib := range rangeCondition.AttributeValueList {
					if attrib.Value(rangeKey.AttributeType) == v1.Value(rangeKey.AttributeType) {
						items = append(items, value)
						break
					}
				}
			case BEGINS_WITH:
				if strings.HasPrefix(v1.Value(rangeKey.AttributeType), v2.Value(rangeKey.AttributeType)) {
					newItems = append(newItems, value)
				}
			case BETWEEN:
				if v1.Value(rangeKey.AttributeType) >= v2.Value(rangeKey.AttributeType) && v1.Value(rangeKey.AttributeType) <= v2.Value(rangeKey.AttributeType) {
					newItems = append(newItems, value)
				}
			}
		}
		items = newItems
	}

	if len(req.ExclusiveStartKey) > 0 {
		newItems = make([]map[string]AttributeValue, 0)
		a := req.ExclusiveStartKey[hashKey.AttributeName]
		startKey := a.Value(hashKey.AttributeType)
		startKeySeen := false
		for _, item := range items {
			if startKeySeen {
				newItems = append(newItems, item)
			} else {
				a = item[hashKey.AttributeName]
				if a.Value(hashKey.AttributeType) == startKey {
					startKeySeen = true
				}
			}
		}
		items = newItems
	}

	// returnItem := make(map[string]AttributeValue)
	// if len(req.AttributesToGet) > 0 {
	// 	for _, attr := range req.AttributesToGet {
	// 		if _, ok := item[attr]; ok {
	// 			returnItem[attr] = item[attr]
	// 		}
	// 	}
	// } else {
	// 	for attr := range item {
	// 		returnItem[attr] = item[attr]
	// 	}
	// }
	result := &QueryResult{
		// Item: returnItem,
		Items: items,
		Count: count,
	}

	if req.ReturnConsumedCapacity == TotalReturnConsumedCapacity {
		result.ConsumedCapacity = t.ConsumedCapacity
	}

	return result, nil
}
