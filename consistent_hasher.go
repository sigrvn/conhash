package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
)

// TODO: change hash datatype to an integer to allow for faster comparisons within BST
// TODO: add multiple hashing functions for each node to more evenly distribute VirtualNodes around hashring
type VirtualNode struct {
	id    string
	hash  string
	left  *VirtualNode
	right *VirtualNode
}

type ConsistentHasher struct {
	root      *VirtualNode
	cache     map[string]string
	nodeCount uint64
}

func NewConsistentHasher() *ConsistentHasher {
	return &ConsistentHasher{
		root:      nil,
		cache:     make(map[string]string),
		nodeCount: 0,
	}
}

func (c *ConsistentHasher) AddNode(nodeId string) {
	digest := md5.Sum([]byte(nodeId))
	nodeHash := hex.EncodeToString(digest[:])
	node := &VirtualNode{
		id:   nodeId,
		hash: nodeHash,
	}

	fmt.Printf("Inserting Node(id: %s, hash: %s)\n", node.id, node.hash)
	c.root = insertNode(c.root, node)
	c.nodeCount += 1
}

func (c *ConsistentHasher) RemoveNode(nodeId string) {
	digest := md5.Sum([]byte(nodeId))
	nodeHash := hex.EncodeToString(digest[:])

	fmt.Printf("Deleting Node(id: %s, hash: %s)\n", nodeId, nodeHash)
	c.root = deleteNode(c.root, nodeHash)
	c.nodeCount -= 1

	// Iterate through all values and evict all keys which are tied to the deleted node
	// NOTE: This operation is relatively expensive (O(n)) but should not happen often as this
	// function only gets called when a Node in the cluster is no longer active
	for key, value := range c.cache {
		if value == nodeId {
			delete(c.cache, key)
		}
	}
}

func (c *ConsistentHasher) FindKey(keyId string) (string, error) {
	if nodeId, found := c.cache[keyId]; found {
		fmt.Printf("Found Key(id: %s) in cache for node: %s\n", keyId, nodeId)
		return nodeId, nil
	}

	digest := md5.Sum([]byte(keyId))
	keyHash := hex.EncodeToString(digest[:])

	fmt.Printf("Finding Node for Key(id: %s, hash: %s)\n", keyId, keyHash)
	node := findNode(c.root, nil, keyHash)
	if node == nil {
		return "", fmt.Errorf("hashring is empty")
	}

	c.cache[keyId] = node.id
	return node.id, nil
}

func (c *ConsistentHasher) NodeCount() uint64 {
	return c.nodeCount
}

func insertNode(root *VirtualNode, node *VirtualNode) *VirtualNode {
	if root == nil {
		return node
	} else if root.hash > node.hash {
		root.left = insertNode(root.left, node)
	} else {
		root.right = insertNode(root.right, node)
	}
	return root
}

func deleteNode(root *VirtualNode, hash string) *VirtualNode {
	if root == nil {
		return root
	} else if root.hash > hash {
		root.left = deleteNode(root.left, hash)
	} else if root.hash < hash {
		root.right = deleteNode(root.right, hash)
	} else {
		if root.left == nil {
			return root.right
		} else if root.right == nil {
			return root.left
		}

		minNode := getMinimumNode(root.right)

		root.id = minNode.id
		root.hash = minNode.hash

		root.right = deleteNode(root.right, root.hash)
	}
	return root
}

func findNode(root *VirtualNode, prev *VirtualNode, hash string) *VirtualNode {
	if root == nil {
		return nil
	}

	if prev != nil {
		if hash < root.hash && hash > prev.hash {
			return root
		} else if hash > root.hash && hash < prev.hash {
			return prev
		}
	}

	prev = root
	if root.hash > hash {
		return findNode(root.left, prev, hash)
	} else {
		return findNode(root.right, prev, hash)
	}

	return root
}

func preorder(root *VirtualNode) {
	fmt.Printf("Node(id: %s, hash: %s)\n", root.id, root.hash)
	if root.left != nil {
		preorder(root.left)
	}
	if root.right != nil {
		preorder(root.right)
	}
}

func getMinimumNode(curr *VirtualNode) *VirtualNode {
	for curr.left != nil {
		curr = curr.left
	}
	return curr
}
