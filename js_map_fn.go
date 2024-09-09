//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package sgbucket

import (
	"context"
	"encoding/json"
	"time"

	extism "github.com/extism/go-sdk"
)

const kTaskCacheSize = 4

// A compiled JavaScript 'map' function, API-compatible with Couchbase Server 2.0.
// Based on JSRunner, so this is not thread-safe; use its wrapper JSMapFunction for that.
type jsMapTask struct {
	JSRunner
	output []*ViewRow
}

// Compiles a JavaScript map function to a jsMapTask object.
func newJsMapTask(funcSource string, timeout time.Duration) (JSServerTask, error) {
	mapper := &jsMapTask{}
	err := mapper.Init(funcSource, timeout)
	if err != nil {
		return nil, err
	}

	mapper.DefineNativeFunction("emit", NewNativeFunction(
		"emit",
		func(ctx context.Context, p *extism.CurrentPlugin, stack []uint64) {
			bytes, err := p.ReadBytes(stack[0])
			if err != nil {
				panic(err)
			}

			var row ViewRow
			json.Unmarshal(bytes, &row)

			mapper.output = append(mapper.output, &row)
		},
		[]extism.ValueType{extism.ValueTypePTR},
		[]extism.ValueType{},
	))

	mapper.Before = func() {
		mapper.output = []*ViewRow{}
	}
	mapper.After = func(result []byte, err error) (interface{}, error) {
		output := mapper.output
		mapper.output = nil
		return output, err
	}
	return mapper, nil
}

// JSMapFunction is a thread-safe wrapper around a jsMapTask, i.e. a Couchbase-Server-compatible JavaScript
// 'map' function.
type JSMapFunction struct {
	*JSServer
}

func NewJSMapFunction(ctx context.Context, fnSource string, timeout time.Duration) *JSMapFunction {
	return &JSMapFunction{
		JSServer: NewJSServer(ctx, fnSource, timeout, kTaskCacheSize,
			func(ctx context.Context, fnSource string, timeout time.Duration) (JSServerTask, error) {
				return newJsMapTask(fnSource, timeout)
			}),
	}
}

type FunctionInput struct {
	Doc  string `json:"doc"`
	Meta string `json:"meta"`
}

// Calls a jsMapTask.
func (mapper *JSMapFunction) CallFunction(ctx context.Context, doc string, docid string, vbNo uint32, vbSeq uint64) ([]*ViewRow, error) {
	meta, err := json.Marshal(MakeMeta(docid, vbNo, vbSeq))
	if err != nil {
		return nil, err
	}
	result1, err := mapper.Call(ctx,
		FunctionInput{
			Doc:  doc,
			Meta: string(meta),
		},
	)
	if err != nil {
		return nil, err
	}
	rows := result1.([]*ViewRow)
	for i, _ := range rows {
		rows[i].ID = docid
	}
	return rows, nil
}

// Returns a Couchbase-compatible 'meta' object, given a document ID
func MakeMeta(docid string, vbNo uint32, vbSeq uint64) map[string]interface{} {
	return map[string]interface{}{
		"id":  docid,
		"vb":  uint32(vbNo),  // convert back to well known type
		"seq": uint64(vbSeq), // ditto
	}

}
