#pragma once
#include <vector>
#include <memory>
#include <list>
#include <any>

class Comparator;
class Iterator;

// Return an iterator that provided the union of the data in
// children[0,n-1].  Takes ownership of the child iterators and
// will delete them when the result iterator is deleted.
//
// The result does no duplicate suppression.  I.e., if a particular
// key is present in K child iterators, it will be yielded K times.
//
// REQUIRES: n >= 0
std::shared_ptr<Iterator> newMergingIterator(
        const Comparator *cmp, std::vector<std::shared_ptr<Iterator>> &list, int n);
