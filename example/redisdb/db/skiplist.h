#pragma once

#include<atomic>
#include<cassert>
#include<cstdlib>

template<typename Key, class Comparator>
class SkipList {
private:
	struct Node;
public:
	// Create a new SkipList object that will use "cmp" for comparing keys,
	// and will allocate memory using "*arena".  Objects allocated in the arena
	// must remain allocated for the lifetime of the skiplist object.
	explicit SkipList(Comparator cmp);

	// Insert key into the list.
	// REQUIRES: nothing that compares equal to key is currently in the list.
	void insert(const Key& key);

	// Returns true iff an entry that compares equal to key is in the list.
	bool contains(const Key& key) const;

	// Iteration over the contents of a skip list
	class Iterator {
	public:
		// Initialize an iterator over the specified list.
		// The returned iterator is not valid.
		explicit Iterator(const SkipList* list);

		// Returns true iff the iterator is positioned at a valid node.
		bool valid() const;

		// Returns the key at the current position.
		// REQUIRES: Valid()
		const Key& key() const;

		// Advances to the next position.
		// REQUIRES: Valid()
		void next();

		// Advances to the previous position.
		// REQUIRES: Valid()
		void prev();

		// Advance to the first entry with a key >= target
		void seek(const Key& target);

		// Position at the first entry in list.
		// Final state of iterator is Valid() iff list is not empty.
		void seekToFirst();

		// Position at the last entry in list.
		// Final state of iterator is Valid() iff list is not empty.
		void seekToLast();

	private:
		const SkipList* list;
		Node* node;
		// Intentionally copyable
	};

private:
	enum { kMaxHeight = 12 };

	// Immutable after construction
	Comparator const compare;

	Node* const head;

	// Modified only by Insert().  Read racily by readers, but stale
	// values are ok.
	std::atomic<int> maxheight;   // Height of the entire list

	inline int getMaxHeight() const {
		return maxheight.load(std::memory_order_relaxed);
	}

	// Read/written only by Insert().
	std::default_random_engine rnd;

	Node* newNode(const Key& key, int height);

	int randomHeight();

	bool equal(const Key& a, const Key& b) const { return (compare(a, b) == 0); }

	// Return true if key is greater than the data stored in "n"
	bool keyIsAfterNode(const Key & key, Node * n) const;

	// Return the earliest node that comes at or after key.
	// Return nullptr if there is no such node.
	//
	// If prev is non-null, fills prev[level] with pointer to previous
	// node at "level" for every level in [0..max_height_-1].
	Node* findGreaterOrEqual(const Key & key, Node * *prev) const;

	// Return the latest node with a key< key.
	// Return head_ if there is no such node.
	Node* findLessThan(const Key & key) const;

	// Return the last node in the list.
	// Return head_ if list is empty.
	Node* findLast() const;

	// No copying allowed
	SkipList(const SkipList&);
	void operator=(const SkipList&);
};

// Implementation details follow
template<typename Key, class Comparator>
struct SkipList<Key, Comparator>::Node {
	explicit Node(const Key& k) : key(k) { }

	Key const key;

	// Accessors/mutators for links.  Wrapped in methods so we can
	// add the appropriate barriers as necessary.
	Node* next(int n) {
		assert(n >= 0);
		// Use an 'acquire load' so that we observe a fully initialized
		// version of the returned Node.
		return nextnode[n].load(std::memory_order_acquire);
	}

	void setNext(int n, Node * x) {
		assert(n >= 0);
		// Use a 'release store' so that anybody who reads through this
		// pointer observes a fully initialized version of the inserted node.
		nextnode[n].store(x, std::memory_order_release);
	}

	// No-barrier variants that can be safely used in a few locations.
	Node* noBarrierNext(int n) {
		assert(n >= 0);
		return nextnode[n].load(std::memory_order_relaxed);
	}

	void noBarrierSetNext(int n, Node * x) {
		assert(n >= 0);
		nextnode[n].store(x, std::memory_order_relaxed);
	}

private:
	// Array of length equal to the node height.  next_[0] is lowest level link.
	std::atomic<Node*> nextnode[1];
};

template<typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node*
SkipList<Key, Comparator>::newNode(const Key & key, int height) {

	char* const nodememory = new char[
		sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1)];
	return new (nodememory) Node(key);
}

template<typename Key, class Comparator>
inline SkipList<Key, Comparator>::Iterator::Iterator(const SkipList * list) {
	this->list = list;
	node = nullptr;
}

template<typename Key, class Comparator>
inline bool SkipList<Key, Comparator>::Iterator::valid() const {
	return node != nullptr;
}

template<typename Key, class Comparator>
inline const Key& SkipList<Key, Comparator>::Iterator::key() const {
	assert(valid());
	return node->key;
}

template<typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::next() {
	assert(valid());
	node = node->next(0);
}

template<typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::prev() {
	// Instead of using explicit "prev" links, we just search for the
	// last node that falls before key.
	assert(valid());
	node = list->findLessThan(node->key);
	if (node == list->head) {
		node = nullptr;
	}
}

template<typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::seek(const Key & target) {
	node = list->findGreaterOrEqual(target, nullptr);
}

template<typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::seekToFirst() {
	node = list->head->next(0);
}

template<typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::seekToLast() {
	node = list->findLast();
	if (node == list->head) {
		node = nullptr;
	}
}

template<typename Key, class Comparator>
int SkipList<Key, Comparator>::randomHeight() {
	// Increase height with probability 1 in kBranching
	static const unsigned int kBranching = 4;
	int height = 1;
	while (height< kMaxHeight && ((rnd() % kBranching) == 0)) {
		height++;
	}

	assert(height > 0);
	assert(height<= kMaxHeight);
	return height;
}

template<typename Key, class Comparator>
bool SkipList<Key, Comparator>::keyIsAfterNode(const Key & key, Node * n) const {
	// null n is considered infinite
	return (n != nullptr) && (compare(n->key, key)< 0);
}

template<typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node*
SkipList<Key, Comparator>::findGreaterOrEqual(const Key & key,
	Node * *prev) const {
	Node* x = head;
	int level = getMaxHeight() - 1;
	while (true) {
		Node* next = x->next(level);
		if (keyIsAfterNode(key, next)) {
			// Keep searching in this list
			x = next;
		}
		else {
			if (prev != nullptr) prev[level] = x;

			if (level == 0) {
				return next;
			}
			else {
				// Switch to next list
				level--;
			}
		}
	}
}

template<typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node*
SkipList<Key, Comparator>::findLessThan(const Key & key) const {
	Node* x = head;
	int level = getMaxHeight() - 1;
	while (true) {
		assert(x == head || compare(x->key, key)< 0);
		Node * next = x->next(level);
		if (next == nullptr || compare(next->key, key) >= 0) {
			if (level == 0) {
				return x;
			}
			else {
				// Switch to next list
				level--;
			}
		}
		else {
			x = next;
		}
	}
}

template<typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::findLast() const {
	Node* x = head;
	int level = getMaxHeight() - 1;
	while (true) {
		Node* next = x->next(level);
		if (next == nullptr) {
			if (level == 0) {
				return x;
			}
			else {
				// Switch to next list
				level--;
			}
		}
		else {
			x = next;
		}
	}
}

template<typename Key, class Comparator>
SkipList<Key, Comparator>::SkipList(Comparator cmp)
	: compare(cmp),
	head(newNode(0 /* any key will do */, kMaxHeight)),
	maxheight(1),
	rnd(time(0)) {
	for (int i = 0; i< kMaxHeight; i++) {
		head->setNext(i, nullptr);
	}
}

template<typename Key, class Comparator>
void SkipList<Key, Comparator>::insert(const Key & key) {
	// TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
	// here since Insert() is externally synchronized.
	Node* prev[kMaxHeight];
	Node* x = findGreaterOrEqual(key, prev);

	// Our data structure does not allow duplicate insertion
	assert(x == nullptr || !equal(key, x->key));

	int height = randomHeight();
	if (height > getMaxHeight()) {
		for (int i = getMaxHeight(); i< height; i++) {
			prev[i] = head;
		}
		// It is ok to mutate max_height_ without any synchronization
		// with concurrent readers.  A concurrent reader that observes
		// the new value of max_height_ will see either the old value of
		// new level pointers from head_ (nullptr), or a new value set in
		// the loop below.  In the former case the reader will
		// immediately drop to the next level since nullptr sorts after all
		// keys.  In the latter case the reader will use the new node.
		maxheight.store(height, std::memory_order_relaxed);
	}

	x = newNode(key, height);
	for (int i = 0; i< height; i++) {
		// NoBarrier_SetNext() suffices since we will add a barrier when
		// we publish a pointer to "x" in prev[i].
		x->noBarrierSetNext(i, prev[i]->noBarrierNext(i));
		prev[i]->setNext(i, x);
	}
}

template<typename Key, class Comparator>
bool SkipList<Key, Comparator>::contains(const Key & key) const {
	Node* x = findGreaterOrEqual(key, nullptr);
	if (x != nullptr && equal(key, x->key)) {
		return true;
	}
	else {
		return false;
	}
}

