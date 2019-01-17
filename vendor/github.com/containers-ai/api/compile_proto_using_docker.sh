#!/bin/bash

ALAMEA_GRPC_PYTHON_IMAGE_REPO="alameda/grpc_python"
ALAMEA_GRPC_PYTHON_IMAGE_TAG="latest"
ALAMEA_GRPC_PYTHON_IMAGE="$ALAMEA_GRPC_PYTHON_IMAGE_REPO:$ALAMEA_GRPC_PYTHON_IMAGE_TAG"
ALAMEA_GRPC_PYTHON_IMAGE_DOCKERFILE=Dockerfile_gRPC_python
REQUIREMENT_MD5=`md5sum requirements.txt | awk '{print $1}'`

ALAMEA_GRPC_GO_IMAGE_REPO="alameda/grpc_go"
ALAMEA_GRPC_GO_IMAGE_TAG="latest"
ALAMEA_GRPC_GO_IMAGE="$ALAMEA_GRPC_GO_IMAGE_REPO:$ALAMEA_GRPC_GO_IMAGE_TAG"
ALAMEA_GRPC_GO_IMAGE_DOCKERFILE=Dockerfile_gRPC_go

generate_dockerfiles(){
    cat > $ALAMEA_GRPC_PYTHON_IMAGE_DOCKERFILE - <<EOF
FROM python:slim
ARG REQUIREMENT_MD5
ARG DOCKERFILE_MD5
ENV REQUIREMENT_MD5=\$REQUIREMENT_MD5 DOCKERFILE_MD5=\$DOCKERFILE_MD5
COPY requirements.txt .
RUN pip install -r requirements.txt
EOF

    cat > $ALAMEA_GRPC_GO_IMAGE_DOCKERFILE - <<EOF
FROM golang:stretch
ARG DOCKERFILE_MD5
ENV DOCKERFILE_MD5=\$DOCKERFILE_MD5 PROTOC_VER=3.6.1 OS_ARC=linux-x86_64 PROTOC_GEN_GO_VER=v1.2.0
RUN apt-get update && apt-get install unzip -y && \\
curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v\$PROTOC_VER/protoc-\$PROTOC_VER-\$OS_ARC.zip && \\
unzip protoc-\$PROTOC_VER-\$OS_ARC.zip -d /usr/local && rm protoc-\$PROTOC_VER-\$OS_ARC.zip && \\
go get -d -u github.com/golang/protobuf/protoc-gen-go && \\
git -C "\$(go env GOPATH)"/src/github.com/golang/protobuf checkout \$PROTOC_GEN_GO_VER && \\
go install github.com/golang/protobuf/protoc-gen-go && \\
rm -rf /var/lib/apt/lists/*
EOF
}

build_python_image(){
    local DOCKERFILE_MD5=`md5sum $ALAMEA_GRPC_PYTHON_IMAGE_DOCKERFILE | awk '{print $1}'`
    docker build --build-arg REQUIREMENT_MD5=$REQUIREMENT_MD5 --build-arg DOCKERFILE_MD5=$DOCKERFILE_MD5 . -t $ALAMEA_GRPC_PYTHON_IMAGE -f $ALAMEA_GRPC_PYTHON_IMAGE_DOCKERFILE 
}

build_go_image(){
    local DOCKERFILE_MD5=`md5sum $ALAMEA_GRPC_GO_IMAGE_DOCKERFILE | awk '{print $1}'`
    docker build --build-arg DOCKERFILE_MD5=$DOCKERFILE_MD5 . -t $ALAMEA_GRPC_GO_IMAGE -f $ALAMEA_GRPC_GO_IMAGE_DOCKERFILE 
}

compile_grpc_python(){
    echo "Check gRPC python image."
    local DOCKERFILE_MD5=`md5sum $ALAMEA_GRPC_PYTHON_IMAGE_DOCKERFILE | awk '{print $1}'`
    if ! docker images $ALAMEA_GRPC_PYTHON_IMAGE | grep $ALAMEA_GRPC_PYTHON_IMAGE_REPO > /dev/null 2>&1; then
        echo "Build new image $ALAMEA_GRPC_PYTHON_IMAGE";
        build_python_image
    fi
    if ! docker run --rm $ALAMEA_GRPC_PYTHON_IMAGE sh -c "
        if [ \"$REQUIREMENT_MD5\" != \"\$REQUIREMENT_MD5\" ] || [ \"$DOCKERFILE_MD5\" != \"\$DOCKERFILE_MD5\" ]; then
            exit 1;
        fi"; then
        echo "Refresh image $ALAMEA_GRPC_PYTHON_IMAGE";
        docker rmi $ALAMEA_GRPC_PYTHON_IMAGE;
        build_python_image
    fi
    echo "Start compiling proto files to python files."
    docker run --rm -v $(pwd):$(pwd) -w $(pwd) $ALAMEA_GRPC_PYTHON_IMAGE bash -c "for pt in \$(find . | grep \\\.proto\$ | grep -v ^\\\./include | grep -v ^\\\./google);do python -m grpc_tools.protoc -I . -I include/ --python_out=./ --grpc_python_out=./ \$pt; done"
    echo "Finish compiling proto files to python files."
}

compile_grpc_go(){
    echo "Check gRPC go image."
    local DOCKERFILE_MD5=`md5sum $ALAMEA_GRPC_GO_IMAGE_DOCKERFILE | awk '{print $1}'`
    if ! docker images $ALAMEA_GRPC_GO_IMAGE | grep $ALAMEA_GRPC_GO_IMAGE_REPO > /dev/null 2>&1; then
        echo "Build new image $ALAMEA_GRPC_GO_IMAGE";
        build_go_image
    fi
    if ! docker run --rm $ALAMEA_GRPC_GO_IMAGE sh -c "
        if [ \"$DOCKERFILE_MD5\" != \"\$DOCKERFILE_MD5\" ]; then
            exit 1;
        fi"; then
        echo "Refresh image $ALAMEA_GRPC_GO_IMAGE";
        docker rmi $ALAMEA_GRPC_GO_IMAGE;
        build_go_image
    fi
    echo "Start compiling proto files to go files."
    docker run --rm -v $(pwd):$(pwd) -w $(pwd) $ALAMEA_GRPC_GO_IMAGE bash -c "for pt in \$(find . | grep \\\.proto\$ | grep -v ^\\\./include | grep -v ^\\\./google);do protoc -I . -I include/ \$pt --go_out=paths=source_relative,plugins=grpc:.; done"
    echo "Finish compiling proto files to go files."
}

remove_dockerfiles(){
   [ -f $ALAMEA_GRPC_PYTHON_IMAGE_DOCKERFILE ] && rm $ALAMEA_GRPC_PYTHON_IMAGE_DOCKERFILE
   [ -f $ALAMEA_GRPC_GO_IMAGE_DOCKERFILE ] && rm $ALAMEA_GRPC_GO_IMAGE_DOCKERFILE
}

generate_dockerfiles
compile_grpc_python
compile_grpc_go
remove_dockerfiles
