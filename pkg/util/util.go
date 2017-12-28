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
	"bytes"
	"encoding/gob"
	"fmt"
	"golang.org/x/net/context"
	"net"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"runtime"
	"runtime/debug"
	"strings"
	"time"
	"unsafe"
)

func PathExist(path string) bool {
	_, err := os.Stat(path)
	return err == nil || os.IsExist(err)
}

func MinInt(x, y int) int {
	if x <= y {
		return x
	} else {
		return y
	}
}

func ClearStringMemory(src *string) {
	p := (*struct {
		ptr uintptr
		len int
	})(unsafe.Pointer(src))

	l := MinInt(p.len, 32)
	ptr := p.ptr
	for idx := 0; idx < l; idx = idx + 1 {
		b := (*byte)(unsafe.Pointer(ptr))
		*b = 0
		ptr += 1
	}
}

func ClearByteMemory(src []byte) {
	l := MinInt(len(src), 32)
	for idx := 0; idx < l; idx = idx + 1 {
		src[idx] = 0
	}
}

type StringContext struct {
	parentCtx context.Context
	kv        map[string]interface{}
}

func (c *StringContext) Deadline() (deadline time.Time, ok bool) {
	return c.parentCtx.Deadline()
}

func (c *StringContext) Done() <-chan struct{} {
	return c.parentCtx.Done()
}

func (c *StringContext) Err() error {
	return c.parentCtx.Err()
}

func (c *StringContext) Value(key interface{}) interface{} {
	k, ok := key.(string)
	if !ok {
		return c.parentCtx.Value(key)
	}
	return c.kv[k]
}

func (c *StringContext) SetKV(key string, val interface{}) {
	c.kv[key] = val
}

func NewStringContext(ctx context.Context) *StringContext {
	strCtx, ok := ctx.(*StringContext)
	if !ok {
		strCtx = &StringContext{
			parentCtx: ctx,
			kv:        make(map[string]interface{}, 10),
		}
	}
	return strCtx
}

func SetContext(ctx context.Context, key string, val interface{}) context.Context {
	strCtx := NewStringContext(ctx)
	strCtx.SetKV(key, val)
	return strCtx
}

func CloneContext(ctx context.Context) context.Context {
	strCtx := &StringContext{
		parentCtx: ctx,
		kv:        make(map[string]interface{}, 10),
	}

	old, ok := ctx.(*StringContext)
	if !ok {
		return strCtx
	}

	for k, v := range old.kv {
		strCtx.kv[k] = v
	}
	return strCtx
}

func FromContext(ctx context.Context, key string) interface{} {
	return ctx.Value(key)
}

func SetRequestContext(r *http.Request, key string, val interface{}) *http.Request {
	ctx := r.Context()
	ctx = SetContext(ctx, key, val)
	if ctx != r.Context() {
		nr := r.WithContext(ctx)
		*r = *nr
	}
	return r
}

func ParseDomainProject(ctx context.Context) string {
	return ParseDomain(ctx) + "/" + ParseProject(ctx)
}

func ParseTargetDomainProject(ctx context.Context) string {
	return ParseTargetDomain(ctx) + "/" + ParseTargetProject(ctx)
}

func ParseDomain(ctx context.Context) string {
	v, ok := FromContext(ctx, "domain").(string)
	if !ok {
		return ""
	}
	return v
}

func ParseTargetDomain(ctx context.Context) string {
	v, _ := FromContext(ctx, "target-domain").(string)
	if len(v) == 0 {
		return ParseDomain(ctx)
	}
	return v
}

func ParseProject(ctx context.Context) string {
	v, ok := FromContext(ctx, "project").(string)
	if !ok {
		return ""
	}
	return v
}

func ParseTargetProject(ctx context.Context) string {
	v, _ := FromContext(ctx, "target-project").(string)
	if len(v) == 0 {
		return ParseProject(ctx)
	}
	return v
}

func SetDomain(ctx context.Context, domain string) context.Context {
	return SetContext(ctx, "domain", domain)
}

func SetProject(ctx context.Context, project string) context.Context {
	return SetContext(ctx, "project", project)
}

func SetTargetDomain(ctx context.Context, domain string) context.Context {
	return SetContext(ctx, "target-domain", domain)
}

func SetTargetProject(ctx context.Context, project string) context.Context {
	return SetContext(ctx, "target-project", project)
}

func SetDomainProject(ctx context.Context, domain string, project string) context.Context {
	return SetProject(SetDomain(ctx, domain), project)
}

func SetTargetDomainProject(ctx context.Context, domain string, project string) context.Context {
	return SetTargetProject(SetTargetDomain(ctx, domain), project)
}

func GetIPFromContext(ctx context.Context) string {
	v, ok := FromContext(ctx, "x-remote-ip").(string)
	if !ok {
		return ""
	}
	return v
}

func DeepCopy(dst, src interface{}) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(src); err != nil {
		return err
	}
	return gob.NewDecoder(bytes.NewBuffer(buf.Bytes())).Decode(dst)
}

func SafeCloseChan(c chan struct{}) {
	select {
	case _, ok := <-c:
		if ok {
			close(c)
		}
	default:
		close(c)
	}
}

func BytesToStringWithNoCopy(bytes []byte) string {
	return *(*string)(unsafe.Pointer(&bytes))
}

func StringToBytesWithNoCopy(s string) []byte {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	h := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&h))
}

func ListToMap(list []string) map[string]struct{} {
	ret := make(map[string]struct{}, len(list))
	for _, v := range list {
		ret[v] = struct{}{}
	}
	return ret
}

func MapToList(dict map[string]struct{}) []string {
	ret := make([]string, 0, len(dict))
	for k := range dict {
		ret = append(ret, k)
	}
	return ret
}

func StringJoin(args []string, sep string) string {
	l := len(args)
	switch l {
	case 0:
		return ""
	case 1:
		return args[0]
	default:
		n := len(sep) * (l - 1)
		for i := 0; i < l; i++ {
			n += len(args[i])
		}
		b := make([]byte, n)
		sl := copy(b, args[0])
		for i := 1; i < l; i++ {
			sl += copy(b[sl:], sep)
			sl += copy(b[sl:], args[i])
		}
		return BytesToStringWithNoCopy(b)
	}
}

func RecoverAndReport() (r interface{}) {
	if r = recover(); r != nil {
		LogPanic(r)
	}
	return
}

// this function can only be called in recover().
func LogPanic(args ...interface{}) {
	for i := 2; i < 10; i++ {
		file, method, line, ok := GetCaller(i)
		if !ok {
			break
		}

		if strings.Index(file, "service-center") > 0 || strings.Index(file, "servicecenter") > 0 {
			idx := strings.LastIndex(file, "/")
			if idx >= 0 {
				file = file[idx+1:]
			}
			Logger().Errorf(nil, "recover from %s %s():%d! %s", file, method, line, fmt.Sprint(args...))
			return
		}
	}

	file, method, line, _ := GetCaller(0)
	idx := strings.LastIndex(file, "/")
	if idx >= 0 {
		file = file[idx+1:]
	}
	fmt.Fprintln(os.Stderr, time.Now().Format("2006-01-02T15:04:05.000Z07:00"), "FATAL", "system", os.Getpid(),
		fmt.Sprintf("%s %s():%d", file, method, line), fmt.Sprint(args...))
	fmt.Fprintln(os.Stderr, BytesToStringWithNoCopy(debug.Stack()))
}

func GetCaller(skip int) (string, string, int, bool) {
	pc, file, line, ok := runtime.Caller(skip + 1)
	method := FormatFuncName(runtime.FuncForPC(pc).Name())
	return file, method, line, ok
}

func ParseEndpoint(ep string) (string, error) {
	u, err := url.Parse(ep)
	if err != nil {
		return "", err
	}
	port := u.Port()
	if len(port) > 0 {
		return u.Hostname() + ":" + port, nil
	}
	return u.Hostname(), nil
}

func GetRealIP(r *http.Request) string {
	for _, h := range [2]string{"X-Forwarded-For", "X-Real-Ip"} {
		addresses := strings.Split(r.Header.Get(h), ",")
		for _, ip := range addresses {
			ip = strings.TrimSpace(ip)
			realIP := net.ParseIP(ip)
			if !realIP.IsGlobalUnicast() {
				continue
			}
			return ip
		}
	}
	addrs := strings.Split(r.RemoteAddr, ":")
	if len(addrs) > 0 {
		return addrs[0]
	}
	return ""
}

func BytesToInt32(bs []byte) (in int32) {
	l := len(bs)
	if l > 4 || l == 0 {
		return 0
	}

	pi := (*[4]byte)(unsafe.Pointer(&in))
	if IsBigEndian() {
		for i := range bs {
			pi[i] = bs[l-i-1]
		}
		return
	}

	for i := range bs {
		pi[3-i] = bs[l-i-1]
	}
	return
}

func UrlEncode(keys map[string]string) string {
	l := len(keys)
	if l == 0 {
		return ""
	}
	arr := make([]string, 0, l)
	for k, v := range keys {
		arr = append(arr, url.QueryEscape(k)+"="+url.QueryEscape(v))
	}
	return StringJoin(arr, "&")
}

func FormatFuncName(f string) string {
	i := strings.LastIndex(f, "/")
	j := strings.Index(f[i+1:], ".")
	if j < 1 {
		return "???"
	}
	_, fun := f[:i+j+1], f[i+j+2:]
	i = strings.LastIndex(fun, ".")
	return fun[i+1:]
}

func FuncName(f interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
}
