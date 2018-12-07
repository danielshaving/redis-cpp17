#pragma once
#include "versionset.h"
#include "dbformat.h"

class findFileTest
{
public:
	std::vector<std::shared_ptr<FileMetaData>> files;
	bool disjointSortFiles;
	findFileTest():disjointSortFiles(true) { }
	~findFileTest()
	{
	
	}

	void add(const char *smallest, const char *largest,
	uint64_t smallestSeq = 100,
	uint64_t largestSeq = 100) 
	{
		std::shared_ptr<FileMetaData> f(new FileMetaData);
		f->number = files.size() + 1;
		f->smallest = InternalKey(smallest, smallestSeq, kTypeValue);
		f->largest = InternalKey(largest, largestSeq, kTypeValue);
		files.push_back(f);
	}

	int find(const char *key)
	{
		InternalKey target(key, 100, kTypeValue);
		std::shared_ptr<Comparator> impl(new BytewiseComparatorImpl());
		InternalKeyComparator cmp(impl.get());
		return findFile(cmp, files, target.encode());
	}

	bool overlaps(const char* smallest, const char* largest) 
	{
		std::shared_ptr<Comparator> impl(new BytewiseComparatorImpl());
		InternalKeyComparator cmp(impl.get());
		std::string_view s(smallest != nullptr ? smallest : "");
		std::string_view l(largest != nullptr ? largest : "");
		return someFileOverlapsRange(cmp, disjointSortFiles, files,
		                         (smallest != nullptr ? &s : nullptr),
		                         (largest != nullptr ? &l : nullptr));
	}
	  
	 void empty()
	 {
	 	assert(find("foo") == 0);
		assert(!overlaps("a", "z"));
		assert(! overlaps(nullptr, "z"));
		assert(! overlaps("a", nullptr));
		assert(! overlaps(nullptr, nullptr));
	 }

	 void single()
	 {
	 	add("p", "q");
		assert(find("a") == 0);
		assert(find("p") == 0);
		assert(find("p1") == 0);
		assert(find("q") == 0);
		assert(find("q1") == 1);
		assert(find("z") == 1);

		assert(! overlaps("a", "b"));
		assert(! overlaps("z1", "z2"));
		assert(overlaps("a", "p"));
		assert(overlaps("a", "q"));
		assert(overlaps("a", "z"));
		assert(overlaps("p", "p1"));
		assert(overlaps("p", "q"));
		assert(overlaps("p", "z"));
		assert(overlaps("p1", "p2"));
		assert(overlaps("p1", "z"));
		assert(overlaps("q", "q"));
		assert(overlaps("q", "q1"));

		assert(! overlaps(nullptr, "j"));
		assert(! overlaps("r", nullptr));
		assert(overlaps(nullptr, "p"));
		assert(overlaps(nullptr, "p1"));
		assert(overlaps("q", nullptr));
		assert(overlaps(nullptr, nullptr));
	 }

	 void multiple()
	 {
		add("150", "200");
		add("200", "250");
		add("300", "350");
		add("400", "450");

		assert(0 == find("100"));
		assert(0 == find("150"));
		assert(0 == find("151"));
		assert(0 == find("199"));
		assert(0 == find("200"));
		assert(1 == find("201"));
		assert(1 == find("249"));
		assert(1 == find("250"));
		assert(2 == find("251"));
		assert(2 == find("299"));
		assert(2 == find("300"));
		assert(2 == find("349"));
		assert(2 == find("350"));
		assert(3 == find("351"));
		assert(3 == find("400"));
		assert(3 == find("450"));
		assert(4 == find("451"));

		assert(! overlaps("100", "149"));
		assert(! overlaps("251", "299"));
		assert(! overlaps("451", "500"));
		assert(! overlaps("351", "399"));

		assert(overlaps("100", "150"));
		assert(overlaps("100", "200"));
		assert(overlaps("100", "300"));
		assert(overlaps("100", "400"));
		assert(overlaps("100", "500"));
		assert(overlaps("375", "400"));
		assert(overlaps("450", "450"));
		assert(overlaps("450", "500"));
	 }

	 void multipleNullBoundarie()
	 {
		add("150", "200");
		add("200", "250");
		add("300", "350");
		add("400", "450");

		assert(! overlaps(nullptr, "149"));
		assert(! overlaps("451", nullptr));
		assert(overlaps(nullptr, nullptr));
		assert(overlaps(nullptr, "150"));
		assert(overlaps(nullptr, "199"));
		assert(overlaps(nullptr, "200"));
		assert(overlaps(nullptr, "201"));
		assert(overlaps(nullptr, "400"));
		assert(overlaps(nullptr, "800"));
		assert(overlaps("100", nullptr));
		assert(overlaps("200", nullptr));
		assert(overlaps("449", nullptr));
		assert(overlaps("450", nullptr));
	 }

	 void overlapSequenceChecks()
	 {
		add("200", "200", 5000, 3000);
		assert(! overlaps("199", "199"));
		assert(! overlaps("201", "300"));
		assert(overlaps("200", "200"));
		assert(overlaps("190", "200"));
		assert(overlaps("200", "210"));
	 }
};

/*
int main()
{
	findFileTest ftest;
	ftest.empty();
	ftest.single();
	return 0;
}
*/

