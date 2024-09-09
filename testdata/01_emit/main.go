// Note: run `go doc -all` in this package to see all of the types and functions available.
// ./pdk.gen.go contains the domain types from the host where your plugin will run.
package main

// Transforms the given record.
// It takes DataRecord as input (A data record)
func Transform(input DataRecord) error {
	Emit(EmitRecord{Key: "key", Value: "value"})
	Emit(EmitRecord{Key: "k2", Value: "v2"})
	return nil
}
