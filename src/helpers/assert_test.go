package helpers

import (
	"testing"
)

func TestAssert(t *testing.T) {
	// Test case where condition is true (should not panic)
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Assert panicked on true condition")
			}
		}()
		Assert(true, "This should not panic")
	}()

	// Test case where condition is false (should panic)
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("Assert did not panic on false condition")
			}
		}()
		Assert(false, "This should panic")
	}()
}
