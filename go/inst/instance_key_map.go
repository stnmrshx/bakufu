/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package inst

import (
	"encoding/json"
	"strings"
)

type InstanceKeyMap map[InstanceKey]bool

func NewInstanceKeyMap() *InstanceKeyMap {
	return &InstanceKeyMap{}
}

func (this *InstanceKeyMap) AddKey(key InstanceKey) {
	(*this)[key] = true
}

func (this *InstanceKeyMap) AddKeys(keys []InstanceKey) {
	for _, key := range keys {
		this.AddKey(key)
	}
}

func (this *InstanceKeyMap) AddInstances(instances [](*Instance)) {
	for _, instance := range instances {
		this.AddKey(instance.Key)
	}
}

func (this *InstanceKeyMap) HasKey(key InstanceKey) bool {
	_, ok := (*this)[key]
	return ok
}

func (this *InstanceKeyMap) GetInstanceKeys() []InstanceKey {
	res := []InstanceKey{}
	for key := range *this {
		res = append(res, key)
	}
	return res
}

func (this *InstanceKeyMap) MarshalJSON() ([]byte, error) {
	return json.Marshal(this.GetInstanceKeys())
}

func (this *InstanceKeyMap) ToJSON() (string, error) {
	bytes, err := this.MarshalJSON()
	return string(bytes), err
}

func (this *InstanceKeyMap) ToJSONString() string {
	s, _ := this.ToJSON()
	return s
}

func (this *InstanceKeyMap) ToCommaDelimitedList() string {
	keyDisplays := []string{}
	for key := range *this {
		keyDisplays = append(keyDisplays, key.DisplayString())
	}
	return strings.Join(keyDisplays, ",")
}

func (this *InstanceKeyMap) ReadJson(jsonString string) error {
	var keys []InstanceKey
	err := json.Unmarshal([]byte(jsonString), &keys)
	if err != nil {
		return err
	}
	this.AddKeys(keys)
	return err
}

func (this *InstanceKeyMap) ReadCommaDelimitedList(list string) error {
	tokens := strings.Split(list, ",")
	for _, token := range tokens {
		key, err := ParseInstanceKey(token)
		if err != nil {
			return err
		}
		this.AddKey(*key)
	}
	return nil
}
