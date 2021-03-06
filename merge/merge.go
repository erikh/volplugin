package merge

import (
	"reflect"

	"github.com/contiv/errored"
)

// Opts is used to merge docker's driver options (which are flat) with our
// options data structures (which are not).
//
// most of the work is done in setKey() and setValueWithType().
//
func Opts(v interface{}, opts map[string]string) error {
	for key, value := range opts {
		ptrVal := reflect.ValueOf(v)
		if err := setKey(reflect.TypeOf(v).Elem(), &ptrVal, key, value); err != nil {
			return err
		}
	}

	return nil
}

// setKey sets a key to a value within the struct. Requires the reflect data +
// key/value.
//
// See the comments in the function for an explanation of what happens.
//
func setKey(typeinfo reflect.Type, valinfo *reflect.Value, key string, value string) error {
	// walk all fields in the struct
	for x := 0; x < typeinfo.NumField(); x++ {
		field := typeinfo.Field(x)

		// if we're a struct, recurse. Do not do this for zero element structs,
		// reflect gets a little confused by these.
		if field.Type.Kind() == reflect.Struct {
			if field.Type.NumField() > 0 {
				var valfield reflect.Value

				if valinfo.Kind() == reflect.Ptr {
					valfield = valinfo.Elem().Field(x)
				} else {
					valfield = valinfo.Field(x)
				}

				err := setKey(field.Type, &valfield, key, value)

				// if the error is non-nil, we don't have a match. Try looping.
				if err != nil {
					continue
				}
				return nil
			}
		}

		// merge tag handling, see mergeOpts comments.
		if field.Tag.Get("merge") == key {
			var valfield reflect.Value

			// some of the values will be pointers, and some won't. The reflect
			// package panics on any invalid use of values, so we need to check
			// first.
			if valinfo.Kind() == reflect.Ptr {
				valfield = valinfo.Elem().Field(x)
			} else {
				valfield = valinfo.Field(x)
			}
			return setValueWithType(&valfield, value)
		}
	}

	return errored.Errorf("Key not found: %q", key)
}

func setValueWithType(field *reflect.Value, val string) error {
	if !field.CanSet() {
		return errored.Errorf("Cannot set value %q for struct element %q", val, field.Kind().String())
	}

	// navigate the kinds using the reflect types. fallthrough until we can get
	// at a convertible type. If nothing is applicable, error out.
	switch field.Kind() {
	case reflect.Int:
		fallthrough
	case reflect.Int32:
		return castInt32(field, val)
	case reflect.Int64:
		return castInt64(field, val)
	case reflect.Uint:
		fallthrough
	case reflect.Uint32:
		return castUint32(field, val)
	case reflect.Uint64:
		return castUint64(field, val)
	case reflect.Bool:
		return castBool(field, val)
	case reflect.Ptr:
		return castPtr(field, val)
	case reflect.String:
		return castString(field, val)
	}

	return errored.Errorf("Could not find appropriate type %q", field.Kind().String())
}
