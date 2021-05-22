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

package exception

import (
	"github.com/apache/servicecomb-service-center/pkg/chain"
	"github.com/apache/servicecomb-service-center/pkg/errors"
	"github.com/apache/servicecomb-service-center/pkg/log"
	"github.com/apache/servicecomb-service-center/pkg/rest"
	"github.com/apache/servicecomb-service-center/pkg/util"
	"github.com/go-chassis/cari/discovery"
	"net/http"
	"strconv"
)

// Handler provide a common response writer to handle exceptions
type Handler struct {
}

func (l *Handler) Handle(i *chain.Invocation) {
	w, r := i.Context().Value(rest.CtxResponse).(http.ResponseWriter),
		i.Context().Value(rest.CtxRequest).(*http.Request)

	asyncWriter := NewWriter(w)
	util.SetRequestContext(r, rest.CtxResponse, asyncWriter)

	i.Next(chain.WithFunc(func(ret chain.Result) {
		if !ret.OK {
			l.responseError(w, ret.Err)
			return
		}

		if err := asyncWriter.Flush(); err != nil {
			log.Error("response writer flush failed", err)
		}
	}))
}

func (l *Handler) responseError(w http.ResponseWriter, e error) {
	statusCode := http.StatusBadRequest
	contentType := rest.ContentTypeText
	body := []byte("Unknown error")
	defer func() {
		w.Header().Set(rest.HeaderContentType, contentType)
		w.Header().Set(rest.HeaderResponseStatus, strconv.Itoa(statusCode))
		w.WriteHeader(statusCode)
		if _, writeErr := w.Write(body); writeErr != nil {
			log.Error("write response failed", writeErr)
		}
	}()

	if e == nil {
		log.Warn("callback result is failure but no error")
		return
	}

	body = util.StringToBytesWithNoCopy(e.Error())
	switch err := e.(type) {
	case errors.InternalError:
		statusCode = http.StatusInternalServerError
	case *discovery.Error:
		statusCode = err.StatusCode()
		contentType = rest.ContentTypeJSON
		body = err.Marshal()
	}
}

func RegisterHandlers() {
	chain.RegisterHandler(rest.ServerChainName, &Handler{})
}
