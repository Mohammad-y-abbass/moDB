package storage

import "bytes"

func binarySearch(node Node, target []byte) (int, bool) {
	low := 0
	high := int(node.nkeys()) - 1
	for low <= high {
		mid := low + (high-low)/2
		cmp := bytes.Compare(node.getKey(uint16(mid)), target)
		if cmp == 0 {
			return mid, true // Found it exactly!
		} else if cmp < 0 {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return low, false // Not found, but 'low' is the insertion point!
}

func nodeLookupLE(node Node, key []byte) uint16 {
	index, found := binarySearch(node, key)
	if found {
		return uint16(index)
	}

	if index > 0 {
		return uint16(index - 1)
	}
	return 0
}
