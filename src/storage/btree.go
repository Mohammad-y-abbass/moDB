package storage

type BTree struct {
	// pointer to the page id of the root node
	root uint64

	// callbacks for managing pages
	get func(uint64) Node
	new func(Node) uint64
	del func(uint64)
}
