# api

This repository defines interfaces for Alameda to access data sources and store its generated predictions and recommendations. The backend that implements these interfaces is the *datahub* component of Alameda. See [Alameda architecture design](https://github.com/containers-ai/alameda/blob/master/design/architecture.md) for more details.

## How to compile

We provide two ways to compile proto files: [within docker environment](#compile-within-docker-environment) and [without docker environment](#compile-without-docker-environment).

### Compile within docker environment

Run the following script to compile proto files with docker
```bash
./compile_proto_using_docker.sh
```
The generated code will be located in the same folder as the .proto files.

### Compile without docker environment

#### Prerequisition

1. Install [Python3](https://www.python.org/downloads/)
2. Install pip3
```bash
sudo apt-get -y install python3-pip
```
3. Install [Go](https://golang.org/dl/)
4. Install protoc-gen-go
```bash
go get -u github.com/golang/protobuf/protoc-gen-go
```
5. Install [protoc](https://github.com/protocolbuffers/protobuf/releases)
6. Install the packages for generating python code
```bash
pip3 install -r requirements.txt
```

#### Compile 

After above steps, run the following script to compile proto files with protoc
```bash
./compile_proto.sh
```

The generated code will be located in the same folder as the .proto files.

## How to use

### Coding with golang

Add the following import declarations in your .go files when using the Alameda API gRPC calls.
```
import "github.com/containers-ai/api/alameda_api/v1alpha1/ai_service"
import "github.com/containers-ai/api/alameda_api/v1alpha1/operator"
```

## Coding with python

Install alameda-api packages by
```bash
pip install git+https://github.com/containers-ai/api.git
```
Then you can use Alameda API gRPC calls in your .py files by
```
from alameda_api.v1alpha1.ai_service import ai_service_pb2, ai_service_pb2_grpc
from alameda_api.v1alpha1.operator import server_pb2, server_pb2_grpc

```
