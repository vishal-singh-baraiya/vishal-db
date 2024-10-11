package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/fatih/color"
)

type BPlusTreeNode[K comparable, V any] struct {
	keys     []K
	values   []V
	children []*BPlusTreeNode[K, V]
	isLeaf   bool
	next     *BPlusTreeNode[K, V] // Link to the next leaf node for easier traversal
	order    int
}

type BPlusTree[K comparable, V any] struct {
	root  *BPlusTreeNode[K, V]
	order int
	less  func(K, K) bool
	equal func(K, K) bool
}

func newBPlusTreeNode[K comparable, V any](order int) *BPlusTreeNode[K, V] {
	return &BPlusTreeNode[K, V]{
		keys:     []K{},
		values:   []V{},
		children: []*BPlusTreeNode[K, V]{},
		isLeaf:   true,
		order:    order,
		next:     nil,
	}
}

func NewBPlusTree[K comparable, V any](order int, less func(K, K) bool, equal func(K, K) bool) *BPlusTree[K, V] {
	return &BPlusTree[K, V]{
		root:  newBPlusTreeNode[K, V](order),
		order: order,
		less:  less,
		equal: equal,
	}
}

func (n *BPlusTreeNode[K, V]) insertNonFull(k K, v V, less func(K, K) bool) {
	i := len(n.keys) - 1

	if n.isLeaf {
		n.keys = append(n.keys, k)
		n.values = append(n.values, v)
		for i >= 0 && less(k, n.keys[i]) {
			n.keys[i+1] = n.keys[i]
			n.values[i+1] = n.values[i]
			i--
		}
		n.keys[i+1] = k
		n.values[i+1] = v
	} else {
		for i >= 0 && less(k, n.keys[i]) {
			i--
		}
		i++
		if len(n.children[i].keys) == 2*(n.order-1) {
			n.splitChild(i, less)
			if less(n.keys[i], k) {
				i++
			}
		}
		n.children[i].insertNonFull(k, v, less)
	}
}

func (n *BPlusTreeNode[K, V]) splitChild(i int, less func(K, K) bool) {
	order := n.order
	y := n.children[i]
	z := newBPlusTreeNode[K, V](order)
	z.isLeaf = y.isLeaf
	z.keys = append(z.keys, y.keys[order:]...)
	z.values = append(z.values, y.values[order:]...)
	y.keys = y.keys[:order-1]
	y.values = y.values[:order-1]

	if y.isLeaf {
		// Correctly link the leaf nodes
		z.next = y.next
		y.next = z
	}

	if !y.isLeaf {
		z.children = append(z.children, y.children[order:]...)
		y.children = y.children[:order]
	}
	n.children = append(n.children[:i+1], append([]*BPlusTreeNode[K, V]{z}, n.children[i+1:]...)...)
	n.keys = append(n.keys[:i], append([]K{y.keys[order-1]}, n.keys[i:]...)...)
}

// Search function to check if a key already exists
func (t *BPlusTree[K, V]) Search(key K) (V, bool) {
	current := t.root
	for current != nil {
		idx := 0
		// Find the index where the key could be
		for idx < len(current.keys) && t.less(current.keys[idx], key) {
			idx++
		}

		// If the key matches, return the value
		if idx < len(current.keys) && t.equal(current.keys[idx], key) {
			return current.values[idx], true

		}

		// If we are at a leaf node, the key was not found
		if current.isLeaf {
			break
		}

		// Move to the appropriate child node
		current = current.children[idx]
	}
	return *new(V), false // Return false if the key is not found
}

// Modify Insert function to check for key uniqueness before insertion
func (t *BPlusTree[K, V]) Insert(key K, value V) {
	// Check if the key already exists
	_, found := t.Search(key)
	if found {
		fmt.Printf("Key %v already exists. Insertion aborted.\n", key)
		return
	}

	// Proceed with normal insertion if key is unique
	root := t.root
	if len(root.keys) == 2*(t.order-1) {
		newRoot := newBPlusTreeNode[K, V](t.order)
		newRoot.isLeaf = false
		newRoot.children = append(newRoot.children, root)
		newRoot.splitChild(0, t.less)
		newRoot.insertNonFull(key, value, t.less)
		t.root = newRoot
	} else {
		root.insertNonFull(key, value, t.less)
	}
}

func (t *BPlusTree[K, V]) Traverse() {
	if t.root == nil {
		fmt.Println("Tree is empty")
		return
	}

	// Start at the leftmost leaf
	current := t.root
	for !current.isLeaf {
		current = current.children[0]
	}

	// Table header
	fmt.Println(strings.Repeat("-", 80))
	fmt.Printf("%-6s | %-10s | %-10s\n", "Index", "Key", "Value")
	fmt.Println(strings.Repeat("-", 80))

	// Traverse through the linked list of leaf nodes
	index := 0
	for current != nil {
		for i := 0; i < len(current.keys); i++ {
			// Print in table format: Index, Key, Value
			fmt.Printf("%-6d | %-10v | %-10v\n", index, current.keys[i], current.values[i])
			fmt.Println(strings.Repeat(".", 80))
			index++
		}
		current = current.next
	}
}

func (t *BPlusTree[K, V]) Delete(key K) {
	t.root.deleteKey(key, t.order, t.less, t.equal)

	if len(t.root.keys) == 0 {
		if !t.root.isLeaf {
			t.root = t.root.children[0]
		} else {
			t.root = nil
		}
	}
}

func (n *BPlusTreeNode[K, V]) deleteKey(key K, order int, less func(K, K) bool, equal func(K, K) bool) {
	idx := n.findKey(key, less)

	if idx < len(n.keys) && equal(n.keys[idx], key) {
		if n.isLeaf {
			n.keys = append(n.keys[:idx], n.keys[idx+1:]...)
			n.values = append(n.values[:idx], n.values[idx+1:]...)
		} else {
			n.deleteFromInternal(idx, order, less, equal)
		}
	} else {
		if n.isLeaf {
			return
		}
		flag := (idx == len(n.keys))
		if len(n.children[idx].keys) < order {
			n.fill(idx, order, less)
		}
		if flag && idx > len(n.keys) {
			n.children[idx-1].deleteKey(key, order, less, equal)
		} else {
			n.children[idx].deleteKey(key, order, less, equal)
		}
	}
}

func (n *BPlusTreeNode[K, V]) findKey(key K, less func(K, K) bool) int {
	idx := 0
	for idx < len(n.keys) && less(n.keys[idx], key) {
		idx++
	}
	return idx
}

func (n *BPlusTreeNode[K, V]) deleteFromInternal(idx int, order int, less func(K, K) bool, equal func(K, K) bool) {
	key := n.keys[idx]
	if len(n.children[idx].keys) >= order {
		pred := n.getPredecessor(idx)
		n.keys[idx] = pred
		n.children[idx].deleteKey(pred, order, less, equal)
	} else if len(n.children[idx+1].keys) >= order {
		succ := n.getSuccessor(idx)
		n.keys[idx] = succ
		n.children[idx+1].deleteKey(succ, order, less, equal)
	} else {
		n.merge(idx, order)
		n.children[idx].deleteKey(key, order, less, equal)
	}
}

func (n *BPlusTreeNode[K, V]) getPredecessor(idx int) K {
	cur := n.children[idx]
	for !cur.isLeaf {
		cur = cur.children[len(cur.children)-1]
	}
	return cur.keys[len(cur.keys)-1]
}

func (n *BPlusTreeNode[K, V]) getSuccessor(idx int) K {
	cur := n.children[idx+1]
	for !cur.isLeaf {
		cur = cur.children[0]
	}
	return cur.keys[0]
}

func (n *BPlusTreeNode[K, V]) fill(idx int, order int, less func(K, K) bool) {
	if idx != 0 && len(n.children[idx-1].keys) >= order {
		n.borrowFromPrev(idx)
	} else if idx != len(n.children)-1 && len(n.children[idx+1].keys) >= order {
		n.borrowFromNext(idx)
	} else {
		if idx != len(n.children)-1 {
			n.merge(idx, order)
		} else {
			n.merge(idx-1, order)
		}
	}
}

func (n *BPlusTreeNode[K, V]) borrowFromPrev(idx int) {
	child := n.children[idx]
	sibling := n.children[idx-1]

	child.keys = append([]K{n.keys[idx-1]}, child.keys...)
	child.values = append([]V{n.values[idx-1]}, child.values...)
	n.keys[idx-1] = sibling.keys[len(sibling.keys)-1]
	n.values[idx-1] = sibling.values[len(sibling.keys)-1]
	sibling.keys = sibling.keys[:len(sibling.keys)-1]
	sibling.values = sibling.values[:len(sibling.values)-1]

	if !child.isLeaf {
		child.children = append([]*BPlusTreeNode[K, V]{sibling.children[len(sibling.children)-1]}, child.children...)
		sibling.children = sibling.children[:len(sibling.children)-1]
	}
}

func (n *BPlusTreeNode[K, V]) borrowFromNext(idx int) {
	child := n.children[idx]
	sibling := n.children[idx+1]

	child.keys = append(child.keys, n.keys[idx])
	child.values = append(child.values, n.values[idx])
	n.keys[idx] = sibling.keys[0]
	n.values[idx] = sibling.values[0]
	sibling.keys = sibling.keys[1:]
	sibling.values = sibling.values[1:]

	if !child.isLeaf {
		child.children = append(child.children, sibling.children[0])
		sibling.children = sibling.children[1:]
	}
}

func (n *BPlusTreeNode[K, V]) merge(idx int, order int) {
	child := n.children[idx]
	sibling := n.children[idx+1]

	child.keys = append(child.keys, n.keys[idx])
	child.values = append(child.values, n.values[idx])
	child.keys = append(child.keys, sibling.keys...)
	child.values = append(child.values, sibling.values...)

	if !child.isLeaf {
		child.children = append(child.children, sibling.children...)
	}

	n.keys = append(n.keys[:idx], n.keys[idx+1:]...)
	n.values = append(n.values[:idx], n.values[idx+1:]...)
	n.children = append(n.children[:idx+1], n.children[idx+2:]...)
}

// GET
func (t *BPlusTree[K, V]) Get(key K) (V, bool) {
	return t.Search(key)
}

// Clear resets the B+ Tree to an empty state.
func (t *BPlusTree[K, V]) Clear() {
	t.root = nil
}

func (t *BPlusTree[K, V]) Height() int {
	return t.height(t.root)
}

// Helper method to calculate the height of the tree recursively
func (t *BPlusTree[K, V]) height(node *BPlusTreeNode[K, V]) int {
	if node == nil {
		return 0
	}
	if node.isLeaf {
		return 1
	}
	return 1 + t.height(node.children[0]) // Height is the height of the first child + 1
}

// Update
func (t *BPlusTree[K, V]) Update(key K, value V) error {
	if _, found := t.Get(key); found {
		t.Delete(key)        // First, delete the existing key-value pair
		t.Insert(key, value) // Then insert the new one
		return nil
	}
	return fmt.Errorf("key '%v' not found for update", key)
}

// Exists checks if the given key exists in the B+ Tree.
func (t *BPlusTree[K, V]) Exists(key K) bool {
	_, found := t.Get(key)
	return found
}

func (t *BPlusTree[K, V]) Count() int {
	return t.count(t.root)
}

// Helper method to count keys recursively
func (t *BPlusTree[K, V]) count(node *BPlusTreeNode[K, V]) int {
	if node == nil {
		return 0
	}
	if node.isLeaf {
		return len(node.keys)
	}
	count := 0
	for _, child := range node.children {
		count += t.count(child)
	}
	return count
}

// List retrieves all keys from the B+ Tree.
func (t *BPlusTree[K, V]) List() []K {
	var keys []K
	t.list(t.root, &keys)
	return keys
}

// Helper method to collect keys recursively
func (t *BPlusTree[K, V]) list(node *BPlusTreeNode[K, V], keys *[]K) {
	if node == nil {
		return
	}
	if node.isLeaf {
		*keys = append(*keys, node.keys...)
		return
	}
	for i := 0; i < len(node.keys); i++ {
		t.list(node.children[i], keys)
		*keys = append(*keys, node.keys[i])
	}
	t.list(node.children[len(node.children)-1], keys) // last child
}

// Range retrieves all key-value pairs within a given range.
func (t *BPlusTree[K, V]) Range(start K, end K) map[K]V {
	result := make(map[K]V)
	current := t.root
	for !current.isLeaf {
		current = current.children[0]
	}

	for current != nil {
		for i := 0; i < len(current.keys); i++ {
			if t.less(start, current.keys[i]) && t.less(current.keys[i], end) {
				result[current.keys[i]] = current.values[i]
			}
		}
		current = current.next
	}
	return result
}

// Stats returns the statistics of the B+ Tree.
func (t *BPlusTree[K, V]) Stats() string {
	return fmt.Sprintf("Total keys: %d, Height: %d", t.Count(), t.Height())
}

func main() {
	tree := NewBPlusTree[string, string](3, func(a, b string) bool { return a < b }, func(a, b string) bool { return a == b })

	scanner := bufio.NewScanner(os.Stdin)
	color.Cyan("Welcome to the B+ Tree REPL!")
	color.Yellow("Commands:")
	color.Green("  insert <key> <value> - Insert a key-value pair into the B+ Tree")
	color.Green("  delete <key> - Delete a key from the B+ Tree")
	color.Green("  update <key> <value> - Update the value for a key")
	color.Green("  exists <key> - Check if a key exists")
	color.Green("  count - Get the total number of keys")
	color.Green("  list - List all keys")
	color.Green("  range <start> <end> - Retrieve all key-value pairs within a given range")
	color.Green("  traverse - Traverse the B+ Tree and display the table")
	color.Green("  get <key> - Retrieve a value by key")
	color.Green("  clear - Clear the B+ Tree")
	color.Green("  height - Get the height of the B+ Tree")
	color.Green("  exit - Exit")

	for {
		color.Magenta("TheViχhal 𓅇  >  ")
		scanner.Scan()
		input := scanner.Text()
		parts := strings.Fields(input)
		if len(parts) == 0 {
			continue
		}

		switch parts[0] {
		case "insert":
			if len(parts) != 3 {
				color.Red("Usage: insert <key> <value>")
				continue
			}
			key := parts[1]
			value := parts[2]
			tree.Insert(key, value)
			color.Green("Inserted: %s:%s\n", key, value)

		case "delete":
			if len(parts) != 2 {
				color.Red("Usage: delete <key>")
				continue
			}
			key := parts[1]
			tree.Delete(key)
			color.Green("Deleted: %s\n", key)

		case "update":
			if len(parts) != 3 {
				color.Red("Usage: update <key> <value>")
				continue
			}
			key := parts[1]
			value := parts[2]
			if err := tree.Update(key, value); err != nil {
				color.Red("Error: %s", err)
			} else {
				color.Green("Updated: %s:%s\n", key, value)
			}

		case "exists":
			if len(parts) != 2 {
				color.Red("Usage: exists <key>")
				continue
			}
			key := parts[1]
			if tree.Exists(key) {
				color.Green("Key '%s' exists.\n", key)
			} else {
				color.Red("Key '%s' does not exist.\n", key)
			}

		case "count":
			count := tree.Count()
			color.Green("Total keys: %d\n", count)

		case "list":
			keys := tree.List()
			color.Green("Keys: %v\n", keys)

		case "range":
			if len(parts) != 3 {
				color.Red("Usage: range <start> <end>")
				continue
			}
			start := parts[1]
			end := parts[2]
			pairs := tree.Range(start, end)
			color.Green("Key-Value Pairs in Range:")
			for k, v := range pairs {
				color.Green("  %s: %s\n", k, v)
			}

		case "traverse":
			tree.Traverse()

		case "get":
			if len(parts) != 2 {
				color.Red("Usage: get <key>")
				continue
			}
			key := parts[1]
			value, found := tree.Get(key)
			if found {
				color.Green("Value for key '%s': %v\n", key, value)
			} else {
				color.Red("Key '%s' not found.\n", key)
			}

		case "clear":
			tree.Clear()
			color.Green("B+ Tree cleared.")

		case "height":
			height := tree.Height()
			color.Green("Height of the B+ Tree: %d\n", height)

		case "exit":
			color.Green("Exiting...")
			return

		default:
			color.Red("Unknown command: %s\n", parts[0])
		}
	}
}
