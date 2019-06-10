#include "redisdb.h"
#include "comparator.h"

RedisDB::RedisDB(const Options& options, const std::string& path)
	:options(options),
	path(path) {

}

RedisDB::~RedisDB() {
	bgtasksshouldexit = true;
	bgtaskscondvar.notify_one();
	if (bgthread != nullptr) {
		if (bgthread->joinable()) {
			bgthread->join();
		}		
	}
}

const Comparator* ListsDataKeyComparator() {
	static ListsDataKeyComparatorImpl l;;
	return &l;
}

const Comparator* ZSetsScoreKeyComparator() {
	static ZSetsScoreKeyComparatorImpl z;
	return &z;
}

Status RedisDB::DestoryDB(const std::string path, const Options& options) {

}

Status RedisDB::Open() {
	options.env->CreateDir(path);

	{
		Options ops = options;
		redisstring.reset(new RedisString(this, ops, path + "/strings"));
		Status s = redisstring->Open();
		assert(s.ok());
	}

	{
		Options ops = options;
		redishash.reset(new RedisHash(this, ops, path + "/hash"));
		Status s = redishash->Open();
		assert(s.ok());
	}

	{
		Options ops = options;
		ops.comparator = ZSetsScoreKeyComparator();
		rediszset.reset(new RedisZset(this, ops, path + "/zset"));
		Status s = rediszset->Open();
		assert(s.ok());
	}

	{
		Options ops = options;
		ops.comparator = ListsDataKeyComparator();
		redislist.reset(new RedisList(this, ops, path + "/list"));
		Status s = redislist->Open();
		assert(s.ok());
	}

	{
		redisset.reset(new RedisSet(this, options, path + "/set"));
		Status s = redisset->Open();
		assert(s.ok());
	}
	return Status::OK();
}

int64_t RedisDB::Del(const std::vector<std::string>& keys, std::map<DataType, Status>* typestatus) {
	Status s;
	int64_t count = 0;
	bool iscorruption = false;

	for (const auto& key : keys) {
		// Strings
		Status s = redisstring->Del(key);
		if (s.ok()) {
			count++;
		} else if (!s.IsNotFound()) {
			iscorruption = true;
			(*typestatus)[DataType::kStrings] = s;
		}

		// Hashes
		s = redishash->Del(key);
		if (s.ok()) {
			count++;
		} else if (!s.IsNotFound()) {
			iscorruption = true;
			(*typestatus)[DataType::kHashes] = s;
		}

		// Sets
		s = redisset->Del(key);
		if (s.ok()) {
			count++;
		} else if (!s.IsNotFound()) {
			iscorruption = true;
			(*typestatus)[DataType::kSets] = s;
		}

		// Lists
		s = redislist->Del(key);
		if (s.ok()) {
			count++;
		} else if (!s.IsNotFound()) {
			iscorruption = true;
			(*typestatus)[DataType::kLists] = s;
		}

		// ZSets
		s = rediszset->Del(key);
		if (s.ok()) {
			count++;
		} else if (!s.IsNotFound()) {
			iscorruption = true;
			(*typestatus)[DataType::kZSets] = s;
		}
	}

	if (iscorruption) {
		return -1;
	} else {
		return count;
	}
}

Status RedisDB::BitPos(const std::string_view& key, int32_t bit, int64_t* ret) {
	return redisstring->BitPos(key, bit, ret);
}

Status RedisDB::BitPos(const std::string_view& key, int32_t bit,
		int64_t startoffset, int64_t* ret) {
	return redisstring->BitPos(key, bit, startoffset, ret);		
}

Status RedisDB::BitPos(const std::string_view& key, int32_t bit,
		int64_t startoffset, int64_t endoffset,
		int64_t* ret) {
	return redisstring->BitPos(key, bit, startoffset, endoffset, ret);				
}

Status RedisDB::Decrby(const std::string_view& key, int64_t value, int64_t* ret) {
	return redisstring->Decrby(key, value, ret);		
}

Status RedisDB::Incrby(const std::string_view& key, int64_t value, int64_t* ret) {
	return redisstring->Incrby(key, value, ret);		
}

Status RedisDB::Setex(const std::string_view& key, const std::string_view& value, int32_t ttl) {
	return redisstring->Setex(key, value, ttl);		
}

Status RedisDB::Strlen(const std::string_view& key, int32_t* len) {
	return redisstring->Strlen(key, len);		
}

Status RedisDB::Set(const std::string_view& key,
	const std::string_view& value) {
	return redisstring->Set(key, value);
}

Status RedisDB::Get(const std::string_view& key, std::string* value) {
	return redisstring->Get(key, value);
}

Status RedisDB::GetSet(const std::string_view& key, 
	const std::string_view& value, std::string* oldValue) {
	return redisstring->GetSet(key, value, oldValue);
}

Status RedisDB::Setxx(const std::string_view& key, 
	const std::string_view& value, int32_t* ret, const int32_t ttl) {
	return redisstring->Setxx(key, value, ret, ttl);
}

Status RedisDB::SetBit(const std::string_view& key, 
	int64_t offset, int32_t value, int32_t* ret) {
	return redisstring->SetBit(key, offset, value, ret);
}

Status RedisDB::GetBit(const std::string_view& key, int64_t offset, int32_t* ret) {
	return redisstring->GetBit(key, offset, ret);
}

Status RedisDB::MSet(const std::vector<KeyValue>& kvs) {
	return redisstring->MSet(kvs);
}

Status RedisDB::MGet(const std::vector<std::string>& keys,
	std::vector<ValueStatus>* vss) {
	return redisstring->MGet(keys, vss);
}

Status RedisDB::Setnx(const std::string_view& key,
	const std::string_view& value, int32_t* ret, const int32_t ttl) {
	return redisstring->Setnx(key, value, ret, ttl);
}

Status RedisDB::MSetnx(const std::vector<KeyValue>& kvs, int32_t* ret) {
	return redisstring->MSetnx(kvs, ret);
}

Status RedisDB::Setvx(const std::string_view& key, const std::string_view& value,
	const std::string_view& newValue, int32_t* ret, const int32_t ttl) {
	return redisstring->Setvx(key, value, newValue, ret, ttl);
}

Status RedisDB::Delvx(const std::string_view& key, const std::string_view& value, int32_t* ret) {
	return redisstring->Delvx(key, value, ret);
}

Status RedisDB::Setrange(const std::string_view& key, int64_t startoffset,
	const std::string_view& value, int32_t* ret) {
	return redisstring->Setrange(key, startoffset, value, ret);
}

Status RedisDB::Getrange(const std::string_view& key, int64_t startoffset, int64_t endoffset,
	std::string* ret) {
	return redisstring->Getrange(key, startoffset, endoffset, ret);
}

Status RedisDB::Append(const std::string_view& key, const std::string_view& value, int32_t* ret) {
	return redisstring->Append(key, value, ret);
}

Status RedisDB::BitCount(const std::string_view& key, int64_t startoffset, int64_t endoffset,
				int32_t* ret, bool haveoffset) {
	return redisstring->BitCount(key, startoffset, endoffset, ret, haveoffset);				
}

Status RedisDB::BitOp(BitOpType op, const std::string& destkey,
			const std::vector<std::string>& srckeys, int64_t* ret) {
	return redisstring->BitOp(op, destkey, srckeys, ret);		
}

Status RedisDB::HSet(const std::string_view& key,
	const std::string_view& field, const std::string_view& value, int32_t* res) {
	return redishash->HSet(key, field, value, res);
}

Status RedisDB::HGetall(const std::string_view& key, std::vector<FieldValue>* fvs) {
	return redishash->HGetall(key, fvs);
}

Status RedisDB::HKeys(const std::string_view& key, std::vector<std::string>* fields) {
	
}

Status RedisDB::HMSet(const std::string_view& key,const std::vector<FieldValue>& fvs) {
	return redishash->HMSet(key, fvs);
}

Status RedisDB::HVals(const std::string_view& key, std::vector<std::string>* values) {

}

Status RedisDB::HGet(const std::string_view& key, const std::string_view& field, std::string* value) {
	return redishash->HGet(key, field, value);
}

Status RedisDB::ZAdd(const std::string_view& key,
	const std::vector<ScoreMember>& scoremembers, int32_t* ret) {
	return rediszset->ZAdd(key, scoremembers, ret);
}

int32_t RedisDB::Expire(const std::string_view& key, int32_t ttl,
					std::map<DataType, Status>* typestatus) {
	int32_t ret = 0;
	bool iscorruption = false;

	Status s = redisstring->Expire(key, ttl);
	if (s.ok()) {
		ret++;
	} else if (!s.IsNotFound()) {
		iscorruption = true;
		(*typestatus)[DataType::kStrings] = s;
	}

	// Hash
	s = redishash->Expire(key, ttl);
	if (s.ok()) {
		ret++;
	} else if (!s.IsNotFound()) {
		iscorruption = true;
		(*typestatus)[DataType::kHashes] = s;
	}

	// Sets
	s = redisset->Expire(key, ttl);
	if (s.ok()) {
		ret++;
	} else if (!s.IsNotFound()) {
		iscorruption = true;
		(*typestatus)[DataType::kSets] = s;
	}

	// Lists
	s = redislist->Expire(key, ttl);
	if (s.ok()) {
		ret++;
	} else if (!s.IsNotFound()) {
		iscorruption = true;
		(*typestatus)[DataType::kLists] = s;
	}

	// Zsets
	s = rediszset->Expire(key, ttl);
	if (s.ok()) {
		ret++;
	} else if (!s.IsNotFound()) {
		iscorruption = true;
		(*typestatus)[DataType::kZSets] = s;
	}

	if (iscorruption) {
		return -1;
	} else {
		return ret;
	}					
}

Status RedisDB::SAdd(const std::string_view& key,
	const std::vector<std::string>& members, int32_t* ret) {
	return redisset->SAdd(key, members, ret);
}

Status RedisDB::SCard(const std::string_view& key, int32_t* ret) {
	return redisset->SCard(key, ret);
}

Status RedisDB::Compact(const DataType& type, bool sync) {

}

Status RedisDB::ZCard(const std::string_view& key, int32_t* ret) {
	return rediszset->ZCard(key, ret);
}

Status RedisDB::ZCount(const std::string_view& key,
		double min,
		double max,
		bool leftclose,
		bool rightclose,
		int32_t* ret) {

}


Status RedisDB::ZRange(const std::string_view& key,
		int32_t start,
		int32_t stop,
		std::vector<ScoreMember>* scoremembers) {
	return rediszset->ZRange(key, start, stop, scoremembers);
}

std::map<DataType, int64_t> RedisDB::TTL(const std::string_view& key,
	std::map<DataType, Status>* typestatus) {
	
}

Status RedisDB::Type(const std::string& key, std::string* type) {
}

Status RedisDB::Keys(const std::string& type,
		  const std::string& pattern,
		  std::vector<std::string>* keys) {
	Status s;
	if (type == "hash") {
		return redishash->ScanKeys(pattern, keys);		  
	}
	
}
		  
Status RedisDB::AddBGTask(const BGTask& bgtask) {
	std::unique_lock<std::mutex> lck(bgtasksmutex);
	if (bgtask.type == kAll) {
		std::queue<BGTask> emptyqueue;
		bgtasksqueue.swap(emptyqueue);
	}
	
	bgtasksqueue.push(bgtask);
	bgtaskscondvar.notify_one();
	return Status::OK();
}

Status RedisDB::RunBGTask() {
	BGTask task;
	while (!bgtasksshouldexit) {
		std::unique_lock<std::mutex> lck(bgtasksmutex);
		while (bgtasksqueue.empty() && !bgtasksshouldexit) {
			bgtaskscondvar.wait(lck);
		}

		if (!bgtasksqueue.empty()) {
			task = bgtasksqueue.front();
			bgtasksqueue.pop();
		}

		if (bgtasksshouldexit) {
			return Status::OK();
		}

		if (task.operation == kCleanAll) {
			DoCompact(task.type);
		} else if (task.operation == kCompactKey) {
			CompactKey(task.type, task.argv);
		}
	}
	return Status::OK();
}

Status RedisDB::DoCompact(const DataType& type) {
	if (type != kAll
		&& type != kStrings
		&& type != kHashes
		&& type != kSets
		&& type != kZSets
		&& type != kLists) {
		return Status::InvalidArgument("");
	}
	
	Status s;
	if (type == kStrings) {
		currenttasktype = Operation::kCleanStrings;
		s = redisstring->CompactRange(nullptr, nullptr);
	} else if (type == kHashes) {
		//currenttasktype = Operation::kCleanHashes;
		//s = redishash->CompactRange(nullptr, nullptr);
	} else if (type == kSets) {
		//currenttasktype = Operation::kCleanSets;
		//s = redisset->CompactRange(nullptr, nullptr);
	} else if (type == kZSets) {
		//currenttasktype = Operation::kCleanZSets;
		//s = rediszset->CompactRange(nullptr, nullptr);
	} else if (type == kLists) {
		//currenttasktype = Operation::kCleanLists;
		//s = redislist->CompactRange(nullptr, nullptr);
	} else {
		currenttasktype = Operation::kCleanAll;
		s = redisstring->CompactRange(nullptr, nullptr);
		/*s = redishash->CompactRange(nullptr, nullptr);
		s = redisset->CompactRange(nullptr, nullptr);
		s = rediszset->CompactRange(nullptr, nullptr);
		s = redislist->CompactRange(nullptr, nullptr);
		*/
	}
	
	currenttasktype = Operation::kNone;
	return s;
}

Status RedisDB::CompactKey(const DataType& type, const std::string& key) {
	std::string metastartkey, metaendkey;
	std::string datastartkey, dataendkey;
	CalculateMetaStartAndEndKey(key, &metastartkey, &metaendkey);
	CalculateDataStartAndEndKey(key, &datastartkey, &dataendkey);
	std::string_view view_meta_begin(metastartkey);
	std::string_view view_meta_end(metaendkey);
	std::string_view view_data_begin(datastartkey);
	std::string_view view_data_end(dataendkey);
	if (type == kSets) {
		//redisset->CompactRange(&view_meta_begin, &view_meta_end, kMeta);
		//redisset->CompactRange(&view_data_begin, &view_data_end, kData);
	} else if (type == kZSets) {
		//rediszset->CompactRange(&view_meta_begin, &view_meta_end, kMeta);
		//rediszset->CompactRange(&view_data_begin, &view_data_end, kData);
	} else if (type == kHashes) {
		//redishash->CompactRange(&view_meta_begin, &view_meta_end, kMeta);
		//redishash->CompactRange(&view_data_begin, &view_data_end, kData);
	} else if (type == kLists) {
		//redislist->CompactRange(&view_meta_begin, &view_meta_end, kMeta);
		//redislist->CompactRange(&view_data_begin, &view_data_end, kData);
	}
}

Status RedisDB::StartBGThread() {
	bgthread.reset(new std::thread(std::bind(&RedisDB::RunBGTask, this)));
}