package main

import (
	"fmt"
)

type AttributeValue struct {
	B  string
	BS []string
	N  string
	NS []string
	S  string
	SS []string
}

func (a *AttributeValue) Value(attributeType AttributeType) string {
	switch attributeType {
	case StringAttributeType:
		return a.S
	case NumberAttributeType:
		return a.N
	case BinaryAttributeType:
		return a.B
	default:
		return ""
	}
}

func (a *AttributeValue) ValidateExpectations(attributeType AttributeType, exp ExpectedAttributeValue) error {
	switch attributeType {
	case StringAttributeType:
		if exp.Value.S != a.S {
			return fmt.Errorf("PuItem: Expectation not met: %v", exp)
		}
	case StringSetAttributeType:
		if len(exp.Value.SS) != len(a.SS) {
			return fmt.Errorf("PuItem: Expectation not met: %v", exp)
		}
		for i, v := range exp.Value.SS {
			if v != a.SS[i] {
				return fmt.Errorf("PuItem: Expectation not met: %v", exp)
			}
		}
	case NumberAttributeType:
		if exp.Value.N != a.N {
			return fmt.Errorf("PuItem: Expectation not met: %v", exp)
		}
	case NumberSetAttributeType:
		if len(exp.Value.NS) != len(a.NS) {
			return fmt.Errorf("PuItem: Expectation not met: %v", exp)
		}
		for i, v := range exp.Value.NS {
			if v != a.NS[i] {
				return fmt.Errorf("PuItem: Expectation not met: %v", exp)
			}
		}
	case BinaryAttributeType:
		if exp.Value.B != a.B {
			return fmt.Errorf("PuItem: Expectation not met: %v", exp)
		}
	case BinarySetAttributeType:
		if len(exp.Value.BS) != len(a.BS) {
			return fmt.Errorf("PuItem: Expectation not met: %v", exp)
		}
		for i, v := range exp.Value.BS {
			if v != a.BS[i] {
				return fmt.Errorf("PuItem: Expectation not met: %v", exp)
			}
		}
	}
	return nil
}
