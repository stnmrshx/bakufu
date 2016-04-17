/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package math

func MinInt(i1, i2 int) int {
	if i1 < i2 {
		return i1
	}
	return i2
}

func MaxInt(i1, i2 int) int {
	if i1 > i2 {
		return i1
	}
	return i2
}

func MinInt64(i1, i2 int64) int64 {
	if i1 < i2 {
		return i1
	}
	return i2
}

func MaxInt64(i1, i2 int64) int64 {
	if i1 > i2 {
		return i1
	}
	return i2
}

func MinUInt(i1, i2 uint) uint {
	if i1 < i2 {
		return i1
	}
	return i2
}

func MaxUInt(i1, i2 uint) uint {
	if i1 > i2 {
		return i1
	}
	return i2
}

func MinUInt64(i1, i2 uint64) uint64 {
	if i1 < i2 {
		return i1
	}
	return i2
}

func MaxUInt64(i1, i2 uint64) uint64 {
	if i1 > i2 {
		return i1
	}
	return i2
}

func MinString(i1, i2 string) string {
	if i1 < i2 {
		return i1
	}
	return i2
}

func MaxString(i1, i2 string) string {
	if i1 > i2 {
		return i1
	}
	return i2
}

func TernaryString(condition bool, resTrue string, resFalse string) string {
	if condition {
		return resTrue
	}
	return resFalse
}

func TernaryInt(condition bool, resTrue int, resFalse int) int {
	if condition {
		return resTrue
	}
	return resFalse
}

func AbsInt(i int) int {
	if i >= 0 {
		return i
	}
	return -i
}

func AbsInt64(i int64) int64 {
	if i >= 0 {
		return i
	}
	return -i
}
