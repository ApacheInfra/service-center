/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package util

import (
	"testing"
)

func TestBytesToInt32(t *testing.T) {
	bs := []byte{0, 0, 0, 1}
	i := BytesToUInt32(bs)
	if i != 1 {
		t.Fatalf("BytesToUInt32 failed, %v %d != %d", bs, i, 1)
	}

	bs = []byte{1, 0, 0, 0}
	i = BytesToUInt32(bs)
	if i != 1<<(3*8) {
		t.Fatalf("BytesToUInt32 failed, %v %d != %d", bs, i, 1<<(3*8))
	}

	bs = []byte{1}
	i = BytesToUInt32(bs)
	if i != 1 {
		t.Fatalf("BytesToUInt32 failed, %v %d != %d", bs, i, 1)
	}

	bs = []byte{1, 0}
	i = BytesToUInt32(bs)
	if i != 1<<8 {
		t.Fatalf("BytesToUInt32 failed, %v %d != %d", bs, i, 1<<8)
	}
}

func TestUint16ToUint64(t *testing.T) {
	bs := []uint16{0, 0, 0, 1}
	i := Uint16ToUint64(bs)
	if i != 1 {
		t.Fatalf("Uint16ToUint64 failed, %v %d != %d", bs, i, 1)
	}

	bs = []uint16{1, 0, 0, 0}
	i = Uint16ToUint64(bs)
	if i != 1<<(3*16) {
		t.Fatalf("Uint16ToUint64 failed, %v %d != %d", bs, i, 1<<(3*16))
	}

	bs = []uint16{1}
	i = Uint16ToUint64(bs)
	if i != 1 {
		t.Fatalf("Uint16ToUint64 failed, %v %d != %d", bs, i, 1)
	}

	bs = []uint16{1, 0}
	i = Uint16ToUint64(bs)
	if i != 1<<16 {
		t.Fatalf("Uint16ToUint64 failed, %v %d != %d", bs, i, 1<<16)
	}
}

func TestFileLastName(t *testing.T) {
	n := FileLastName("")
	if n != "" {
		t.Fatal("TestFileLastName '' failed", n)
	}
	n = FileLastName("a")
	if n != "a" {
		t.Fatal("TestFileLastName 'a' failed", n)
	}
	n = FileLastName("a/b")
	if n != "a/b" {
		t.Fatal("TestFileLastName 'a/b' failed", n)
	}
	n = FileLastName("a/b/c")
	if n != "b/c" {
		t.Fatal("TestFileLastName 'b/c' failed", n)
	}
	n = FileLastName("b/")
	if n != "b/" {
		t.Fatal("TestFileLastName 'b' failed", n)
	}
	n = FileLastName("/")
	if n != "/" {
		t.Fatal("TestFileLastName 'b' failed", n)
	}
}
