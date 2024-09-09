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
	"errors"
	"fmt"
	"time"

	extism "github.com/extism/go-sdk"
	"github.com/robertkrimen/otto"
)

// Alternate type to wrap a Go string in to mark that Call() should interpret it as JSON.
// That is, when Call() sees a parameter of type JSONString it will parse the JSON and use
// the result as the parameter value, instead of just converting it to a JS string.
type JSONString string

type NativeFunction extism.HostFunction

// This specific instance will be returned if a call times out.
var ErrJSTimeout = errors.New("javascript function timed out")

// Go interface to a JavaScript function (like a map/reduce/channelmap/validation function.)
// Each JSServer object compiles a single function into a JavaScript runtime, and lets you
// call that function.
// JSRunner is NOT thread-safe! For that, use JSServer, a wrapper around it.
type JSRunner struct {
	hostFns []extism.HostFunction
	fn      *extism.Plugin
	fnInit  func(ctx context.Context) (*extism.Plugin, error)

	fnSource string
	timeout  time.Duration

	// Optional function that will be called just before the JS function.
	Before func()

	// Optional function that will be called after the JS function returns, and can convert
	// its output from JS (Otto) values to Go values.
	After func([]byte, error) (interface{}, error)
}

// Creates a new JSRunner that will run a JavaScript function.
// 'funcSource' should look like "function(x,y) { ... }"
func NewJSRunner(funcSource string, timeout time.Duration) (*JSRunner, error) {
	runner := &JSRunner{}
	if err := runner.Init(funcSource, timeout); err != nil {
		return nil, err
	}
	return runner, nil
}

// Initializes a JSRunner.
func (runner *JSRunner) Init(funcSource string, timeout time.Duration) error {
	return runner.InitWithLogging(funcSource, timeout, defaultLogFunction, defaultLogFunction)
}

func (runner *JSRunner) InitWithLogging(funcSource string, timeout time.Duration, consoleErrorFunc func(string), consoleLogFunc func(string)) error {
	runner.fn = nil
	runner.timeout = timeout

	if _, err := runner.SetFunction(funcSource); err != nil {
		return err
	}

	return nil

}

func defaultLogFunction(s string) {
	fmt.Println(s)
}

// Sets the JavaScript function the runner executes.
func (runner *JSRunner) SetFunction(funcSource string) (bool, error) {
	if funcSource == runner.fnSource {
		return false, nil // no-op
	}
	if funcSource == "" {
		runner.fn = nil
	} else {

		manifest := extism.Manifest{}
		err := json.Unmarshal([]byte(funcSource), &manifest)
		if err != nil {
			return false, err
		}

		runner.fnInit = func(ctx context.Context) (*extism.Plugin, error) {
			config := extism.PluginConfig{
				EnableWasi: true,
			}
			plugin, err := extism.NewPlugin(
				ctx, manifest, config, runner.hostFns)
			if err != nil {
				return nil, err
			}
			return plugin, nil
		}

	}
	runner.fnSource = funcSource
	return true, nil
}

// Sets the runner's timeout. A value of 0 removes any timeout.
func (runner *JSRunner) SetTimeout(timeout time.Duration) {
	runner.timeout = timeout
}

func NewNativeFunction(
	name string,
	callback extism.HostFunctionStackCallback,
	params []extism.ValueType,
	returnTypes []extism.ValueType,
) NativeFunction {
	return NativeFunction(extism.NewHostFunctionWithStack(name, callback, params, returnTypes))
}

// Lets you define native helper functions (for example, the "emit" function to be called by
// JS map functions) in the main namespace of the JS runtime.
// This method is not thread-safe and should only be called before making any calls to the
// main JS function.
func (runner *JSRunner) DefineNativeFunction(name string, function NativeFunction) {
	runner.hostFns = append(runner.hostFns, extism.HostFunction(function))
}

// ToValue calls ToValue on the otto instance.  Required for conversion of
// complex types to otto Values.
func (runner *JSRunner) ToValue(value interface{}) (otto.Value, error) {
	panic("not implemented")
}

// Invokes the JS function with JSON inputs.
func (runner *JSRunner) CallWithJSON(inputs ...string) (interface{}, error) {
	if runner.Before != nil {
		runner.Before()
	}
	var result uint32
	var output []byte
	var err error
	if runner.fnInit != nil {
		runner.fn, err = runner.fnInit(context.Background())
		if err != nil {
			return nil, err
		}
	}
	if runner.fn != nil {
		if len(inputs) > 1 {
			panic("unsupported input size")
		}
		// Make a JSON array out of the arg list
		result, output, err = runner.fn.Call("transform", []byte(inputs[0]))
	}

	if runner.After != nil {
		return runner.After(output, err)
	}

	_ = result
	return nil, err
}

// Invokes the JS function with Go inputs.
func (runner *JSRunner) Call(_ context.Context, inputs ...interface{}) (_ interface{}, err error) {
	if runner.Before != nil {
		runner.Before()
	}

	var result uint32
	var output []byte
	if runner.fnInit != nil {
		runner.fn, err = runner.fnInit(context.Background())
		if err != nil {
			return nil, err
		}
	}
	if runner.fn != nil {

		input, err := json.Marshal(inputs[0])
		if err != nil {
			return nil, err
		}

		timeout := runner.timeout
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		result, output, err = runner.fn.CallWithContext(ctx, "transform", []byte(input))
		if err != nil {
			return nil, err
		}
	}
	if runner.After != nil {
		return runner.After(output, err)
	}

	_ = result
	return nil, err
}
