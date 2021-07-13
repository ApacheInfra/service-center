#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e

# GO flags
export GOPROXY=${GOPROXY:-"https://goproxy.io"}
export GOOS=${GOOS:-"linux"}
export GOARCH=${GOARCH:-"amd64"}
export CGO_ENABLED=${CGO_ENABLED:-0} # prevent to compile cgo file
export GO_EXTLINK_ENABLED=${GO_EXTLINK_ENABLED:-0} # do not use host linker
export GO_LDFLAGS=${GO_LDFLAGS:-" -s -w"}
if [[ $GOOS == "linux" ]]; then
  if [[ $CGO_ENABLED == 1 ]]; then
    export CGO_CFLAGS="${CGO_CFLAGS} -fstack-protector-strong -D_FORTIFY_SOURCE=2 -O2"
  fi
  if [[ $GO_EXTLINK_ENABLED == 1 ]]; then
    export GO_LDFLAGS="${GO_LDFLAGS} -linkmode=external -extldflags \"-Wl,-z,now\""
  fi
fi
# Inputs
export RELEASE=${RELEASE:-"0.0.1"}
export PACKAGE_PREFIX=${PACKAGE_PREFIX:-"apache-servicecomb-service-center"}
export BUILD=${BUILD:-""} # build all if set value 'ALL'

script_path=$(cd "$(dirname "$0")"; pwd)
root_path=${script_path}/../..

fail() {
	echo "$1"
	exit 1
}

install_bower() {
    set +e
    local NPM=$(which npm)
    set -e
    if [ ${NPM}"x" == "x" ]; then
        curl -sL https://deb.nodesource.com/setup_8.x | bash -
        sudo apt-get install -y nodejs
    fi

    set +e
    local BOWER=$(which bower)
    set -e
    if [ ${BOWER}"x" == "x" ]; then
        npm install -g bower
    fi
}

frontend_deps() {
    cd ${script_path}/../../frontend/app
    if [ ! -d "bower_components" ]; then
        ## Download the frontend dependencies using bower
        install_bower
        bower install --allow-root
    fi
    cd -
}

build_frontend() {
    ## Build Frontend Release
    frontend_deps

    local BINARY_NAME=$PACKAGE_PREFIX-$RELEASE-$GOOS-$GOARCH/frontend
    if [ "$GOOS" == "windows" ]; then
        BINARY_NAME=${BINARY_NAME}.exe
    fi
    go build ${GO_BUILDMODE} --ldflags "${GO_LDFLAGS}" -o $BINARY_NAME github.com/apache/servicecomb-service-center/frontend
}

build_service_center() {
    local app=$PACKAGE_PREFIX-$RELEASE-$GOOS-$GOARCH

    set +e
    rm -rf $app
    rm -rf $app.tar.gz

    set -e
    mkdir -p $app

    ## Build the Service-Center releases
    export GIT_COMMIT=$(git log  --pretty=format:'%h' -n 1)
    export BUILD_NUMBER=$RELEASE
    local ldflags="${GO_LDFLAGS} -X 'github.com/apache/servicecomb-service-center/version.BuildTag=$(date +%Y%m%d%H%M%S).$BUILD_NUMBER.$GIT_COMMIT'"
    ldflags="${ldflags} -X 'github.com/apache/servicecomb-service-center/version.VERSION=$BUILD_NUMBER'"
    local BINARY_NAME=$app/service-center
    if [ "$GOOS" == "windows" ]; then
        BINARY_NAME=${BINARY_NAME}.exe
    fi
    go build ${GO_BUILDMODE} --ldflags "${ldflags}" -o $BINARY_NAME github.com/apache/servicecomb-service-center/cmd/scserver
}

build_scctl() {
    local app=$PACKAGE_PREFIX-$RELEASE-$GOOS-$GOARCH
    ## Build the scctl releases
    export GIT_COMMIT=$(git log  --pretty=format:'%h' -n 1)
    export BUILD_NUMBER=$RELEASE
    local ldflags="${GO_LDFLAGS} -X 'github.com/apache/servicecomb-service-center/scctl/pkg/version.BUILD_TAG=$(date +%Y%m%d%H%M%S).$BUILD_NUMBER.$GIT_COMMIT'"
    ldflags="${ldflags} -X 'github.com/apache/servicecomb-service-center/scctl/pkg/version.VERSION=$BUILD_NUMBER'"

    local BINARY_NAME=$app/scctl
    if [ "$GOOS" == "windows" ]; then
        BINARY_NAME=${BINARY_NAME}.exe
    fi
    go build ${GO_BUILDMODE} --ldflags "${ldflags}" -o $BINARY_NAME github.com/apache/servicecomb-service-center/cmd/scctl
}

build_syncer() {
    local app=$PACKAGE_PREFIX-$RELEASE-$GOOS-$GOARCH
    ## Build the syncer releases
    export GIT_COMMIT=$(git log  --pretty=format:'%h' -n 1)
    export BUILD_NUMBER=$RELEASE
    local ldflags="${GO_LDFLAGS} -X 'github.com/apache/servicecomb-service-center/syncer/version.BUILD_TAG=$(date +%Y%m%d%H%M%S).$BUILD_NUMBER.$GIT_COMMIT'"
    ldflags="${ldflags} -X 'github.com/apache/servicecomb-service-center/syncer/version.VERSION=$BUILD_NUMBER'"

    local BINARY_NAME=$app/syncer
    if [ "$GOOS" == "windows" ]; then
        BINARY_NAME=${BINARY_NAME}.exe
    fi
    go build ${GO_BUILDMODE} --ldflags "${ldflags}" -o $BINARY_NAME github.com/apache/servicecomb-service-center/cmd/syncer
}

## Prepare the Configuration and Make package
package() {
    local app=$PACKAGE_PREFIX-$RELEASE-$GOOS-$GOARCH

    cp -r ${root_path}/etc/conf $app/
    cat <<EOF >> $app/conf/app.yaml
SERVER_HOST: 0.0.0.0
REGISTRY_KIND: embedded_etcd
REGISTRY_ETCD_CLUSTER_NAME: sc-0
REGISTRY_ETCD_CLUSTER_MANAGER_ENDPOINTS: http://127.0.0.1:2380
REGISTRY_ETCD_CLUSTER_ENDPOINTS: sc-0=http://127.0.0.1:2380
EOF

    ## Copy the Service-Center Releases
    cp -r ${root_path}/scripts/release/LICENSE $app/
    cp -r ${root_path}/scripts/release/licenses $app/
    cp -r ${root_path}/scripts/release/NOTICE $app/
    cp -r ${root_path}/README.md $app/

    ## Copy the frontend releases
    cp -r ${root_path}/frontend/app $app/

    ## Copy Start Scripts
    cp -r ${root_path}/scripts/release/start_scripts/$GOOS/* $app/
    if [ "$GOOS" != "windows" ]; then
        chmod +x $app/*.sh
    fi

    ## Archive the release
    tar -czvf $app.tar.gz $app
}

build() {
    build_service_center
    if [ "${BUILD}" == "ALL" ]; then
      # tools
      build_scctl
      build_syncer
      # sc frontend
      build_frontend
    fi
    package
}

# Docker common functions

# docker_builder_pattern execute the local build in docker builder and output the package to the specify directory
docker_builder_pattern() {
    local dockerfile_dir=${1:-"."}
    local output=${2:-"."}
    local builder_name=servicecomb/service-center:build
    local builder_path=/go/src/github.com/apache/servicecomb-service-center
    local app=$PACKAGE_PREFIX-$RELEASE-$GOOS-$GOARCH

    set +e

    docker rmi $builder_name

    set -e

    cd $dockerfile_dir
    docker build --build-arg RELEASE --build-arg BUILD -t $builder_name .
    docker create --name builder $builder_name
    docker cp builder:$builder_path/$app $output
    docker rm -f builder
    cd -
}