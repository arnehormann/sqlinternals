// mirror - check ability to convert two types using unsafe
//
// Copyright 2013 Arne Hormann. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mirror

import (
	//"fmt"
	"reflect"
)

// TODO: add variants or configurability:
// - check only offsets of matching named fields (so _ [x]byte padding is allowed)
// - don't check type names
// - ignore names for anonymous structs
// - extract logging into another function (common walk struct function?)

// CanConvertUnsafe returns true if the memory layout and the struct field names of
// 'from' matches those of 'to'. 'from' and 'to' are interchangeable, the test is bidirectional.
func CanConvertUnsafe(from, to reflect.Type, recurseStructs int) bool {
	//fmt.Printf("CHECK:\n -\t%s\n -\t%s\n", from, to)
	if from.Kind() != reflect.Struct || from.Kind() != to.Kind() ||
		from.Name() != to.Name() || from.NumField() != to.NumField() {
		return false
	}
	for i, max := 0, from.NumField(); i < max; i++ {
		sf, tf := from.Field(i), to.Field(i)
		//fmt.Printf("checking [%d]:\n\t%s: %v\n\t%s: %v\n", i, sf.Name, sf.Type, tf.Name, tf.Type)
		if sf.Name != tf.Name || sf.Offset != tf.Offset {
			return false
		}
		tsf, ttf := sf.Type, tf.Type
		for done := false; !done; {
			k := tsf.Kind()
			if k != ttf.Kind() {
				return false
			}
			//fmt.Printf("\t->\t%s %s == %s %s\n", tsf, tsf.Kind(), ttf, ttf.Kind())
			switch k {
			case reflect.Array, reflect.Chan, reflect.Map, reflect.Ptr, reflect.Slice:
				tsf, ttf = tsf.Elem(), ttf.Elem()
			case reflect.Interface:
				// don't have to handle matching interfaces here
				if tsf != ttf {
					// there are none in our case, so we are extra strict
					return false
				}
			case reflect.Struct:
				if recurseStructs <= 0 && tsf.Name() != ttf.Name() {
					return false
				}
				done = true
			default:
				done = true
			}
		}
		if recurseStructs > 0 && !CanConvertUnsafe(tsf, ttf, recurseStructs-1) {
			return false
		}
	}
	//fmt.Printf("CHECK - are castable\n")
	return true
}
