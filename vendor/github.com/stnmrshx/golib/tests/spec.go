package tests

import (
	"testing"
)

type Spec struct {
	t *testing.T
}

func S(t *testing.T) *Spec {
	return &Spec{t: t}
}

func (spec *Spec) ExpectNil(actual interface{}) {
	if actual == nil {
		return
	}
	spec.t.Errorf("Expected %+v to be nil", actual)
}

func (spec *Spec) ExpectNotNil(actual interface{}) {
	if actual != nil {
		return
	}
	spec.t.Errorf("Expected %+v to be not nil", actual)
}

func (spec *Spec) ExpectEquals(actual, value interface{}) {
	if actual == value {
		return
	}
	spec.t.Errorf("Expected %+v, got %+v", value, actual)
}

func (spec *Spec) ExpectNotEquals(actual, value interface{}) {
	if !(actual == value) {
		return
	}
	spec.t.Errorf("Expected not %+v", value)
}

func (spec *Spec) ExpectEqualsAny(actual interface{}, values ...interface{}) {
	for _, value := range values {
		if actual == value {
			return
		}
	}
	spec.t.Errorf("Expected %+v to equal any of given values", actual)
}

func (spec *Spec) ExpectNotEqualsAny(actual interface{}, values ...interface{}) {
	for _, value := range values {
		if actual == value {
			spec.t.Errorf("Expected not %+v", value)
		}
	}
}

func (spec *Spec) ExpectFalse(actual interface{}) {
	spec.ExpectEquals(actual, false)
}

func (spec *Spec) ExpectTrue(actual interface{}) {
	spec.ExpectEquals(actual, true)
}
