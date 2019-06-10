#pragma once
#include "db.h"

struct KeyValue {
	std::string key;
	std::string value;

	bool operator == (const KeyValue& kv) const {
		return (kv.key == key && kv.value == value);
	}

	bool operator< (const KeyValue & kv) const {
		return key < kv.key;
	}
};

struct ValueStatus {
	std::string value;
	Status status;
	bool operator == (const ValueStatus& vs) const {
		return (vs.value == value && vs.status == status);
	}
};

struct FieldValue {
	std::string field;
	std::string value;
	bool operator == (const FieldValue& fv) const {
		return (fv.field == field && fv.value == value);
	}
};

struct ScoreMember {
	double score;
	std::string member;
	bool operator == (const ScoreMember& sm) const {
		return (sm.score == score && sm.member == member);
	}
};

struct KeyInfo {
	uint64_t keys;
	uint64_t expires;
	uint64_t avgttl;
	uint64_t invaildkeys;
};

enum DataType {
	kAll,
	kStrings,
	kHashes,
	kLists,
	kZSets,
	kSets
};

enum BitOpType {
	kBitOpAnd = 1,
	kBitOpOr,
	kBitOpXor,
	kBitOpNot,
	kBitOpDefault
};

enum AGGREGATE {
	SUM,
	MIN,
	MAX
};

enum BeforeOrAfter {
	Before,
	After
};

enum Operation {
	kNone = 0,
	kCleanAll,
	kCleanStrings,
	kCleanHashes,
	kCleanZSets,
	kCleanSets,
	kCleanLists,
	kCompactKey
};

enum ColumnFamilyType {
	kMeta,
	kData,
	kMetaAndData
};


struct BGTask {
	DataType type;
	Operation operation;
	std::string argv;

	BGTask(const DataType& type = DataType::kAll,
			const Operation& opeation = Operation::kNone,
			const std::string& argv = "") : type(type), operation(opeation), argv(argv) {}
};