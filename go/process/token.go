/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package process

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
)

func GetHash(input []byte) string {
	hasher := sha256.New()
	hasher.Write(input)
	return hex.EncodeToString(hasher.Sum(nil))
}

func GetRandomData() []byte {
	size := 64
	rb := make([]byte, size)
	_, _ = rand.Read(rb)
	return rb
}

type Token struct {
	Hash string
}

var ProcessToken *Token = NewToken()

func NewToken() *Token {
	return &Token{
		Hash: GetHash(GetRandomData()),
	}
}
