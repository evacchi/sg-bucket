// Note: run `go doc -all` in this package to see all of the types and functions available.
// ./pdk.gen.go contains the domain types from the host where your plugin will run.
package main

import "encoding/json"

// Transforms the given record.
// It takes DataRecord as input (A data record)
func Transform(input DataRecord) error {
	var doc map[string]string
	if err := json.Unmarshal([]byte(input.Doc), &doc); err != nil {
		return err
	}
	Emit(EmitRecord{Key: doc["key"], Value: doc["value"]})
	return nil
}
