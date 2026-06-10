package bucket

import "github.com/thanos-io/objstore"

// BucketWithExpectedErrs is implemented by bucket clients that support
// skipping retries for expected errors.
type BucketWithExpectedErrs interface {
	WithExpectedErrs(func(error) bool) objstore.Bucket
}
