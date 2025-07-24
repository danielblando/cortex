package ring

import "strings"

// FormatPartitionState returns a string representation of the partition state without the "P_" prefix.
func FormatPartitionState(s PartitionState) string {
	// Get the string representation from the generated code
	str := s.String()

	// Remove the "P_" prefix
	return strings.TrimPrefix(str, "P_")
}
