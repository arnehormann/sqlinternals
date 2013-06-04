// mirror - check ability to convert two types using unsafe
//
// Copyright 2013 Arne Hormann. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mirror

import (
	"reflect"
	"testing"
)

// TODO:
// - each possible type, first with single-type structs
// - interfaces in struct
// - recursion

// the current state is more of a smoke test, but it's also tested by usage in the other packages

type test struct {
	name string
	ok   bool
	a    interface{}
	b    interface{}
}

func TestMirror(t *testing.T) {
	tests := []test{
		test{"empty", true, struct{}{}, struct{}{}},
		test{"uint8", true, struct{ a uint8 }{}, struct{ a uint8 }{}},
		test{"wrong name", false, struct{ a uint8 }{}, struct{ b uint8 }{}},
		test{"wrong type", false, struct{ a uint8 }{}, struct{ a *uint8 }{}},
	}
	for _, x := range tests {
		ta, tb := reflect.TypeOf(x.a), reflect.TypeOf(x.b)
		if x.ok != CanConvertUnsafe(ta, tb, 0) {
			if x.ok {
				t.Errorf("%s: could convert [%v] to [%v]\n",
					x.name, ta, tb)
			} else {
				t.Errorf("%s: could not convert [%v] to [%v]\n",
					x.name, ta, tb)
			}
		}
	}
}
