#!/bin/sh

protoc -I pkg/grpc/model pkg/grpc/model/item.proto --go_out=plugins=grpc:pkg/grpc/model