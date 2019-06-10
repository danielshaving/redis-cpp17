#pragma once
#include <string>

class Histogram {
public:
	Histogram() {}
	~Histogram() {}

	void Clear();
	void Add(double value);
	void Merge(const Histogram& other);

	std::string ToString() const;

private:
	enum { kNumBuckets = 154 };

	double Median() const;
	double Percentile(double p) const;
	double Average() const;
	double StandardDeviation() const;

	static const double kBucketLimit[kNumBuckets];

	double min;
	double max;
	double num;
	double sum;
	double sumsquares;
	double buckets[kNumBuckets];
};