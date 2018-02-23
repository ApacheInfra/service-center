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
package main

import (
	"flag"
	"fmt"
	"github.com/apache/incubator-servicecomb-service-center/frontend/schema"
	"github.com/astaxie/beego"
	"log"
	"net/http"
	//"strconv"
)

func main() {
	frontendIp := beego.AppConfig.String("FRONTEND_HOST_IP")
	frontendPort := beego.AppConfig.DefaultInt("FRONTEND_HOST_PORT", 30103)

	// command line flags
	port := flag.Int("port", frontendPort, "port to serve on")
	dir := flag.String("directory", "app/", "directory of web files")

	flag.Parse()

	// handle all requests by serving a file of the same name
	fs := http.Dir(*dir)
	fileHandler := http.FileServer(fs)
	http.Handle("/", fileHandler)

	schemaHandler := schema.TestSchema()
	http.Handle("/testSchema/", schemaHandler)

	log.Printf("Running on port %d\n", *port)

	addr := fmt.Sprintf("%s:%d", frontendIp, *port)
	// this call blocks -- the progam runs here forever
	fmt.Println(http.ListenAndServe(addr, nil))
}
