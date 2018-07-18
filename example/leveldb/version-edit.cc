#include "version-edit.h"
#include "version-set.h"
#include "coding.h"

// Tag numbers for serialized VersionEdit.  These numbers are written to
// disk and should not be changed.
enum Tag 
{
	kComparator           = 1,
	kLogNumber            = 2,
	kNextFileNumber       = 3,
	kLastSequence         = 4,
	kCompactPointer       = 5,
	kDeletedFile          = 6,
	kNewFile              = 7,
	// 8 was used for large value refs
	kPrevLogNumber        = 9
};

void VersionEdit::clear() 
{
	comparator.clear();
	logNumber = 0;
	prevLogNumber = 0;
	lastSequence = 0;
	nextFileNumber = 0;
	hasComparator = false;
	hasLogNumber = false;
	hasPrevLogNumber = false;
	hasNextFileNumber = false;
	hasLastSequence = false;
	deletedFiles.clear();
	newFiles.clear();
}

void VersionEdit::encodeTo(std::string *dst) const
{
	if (hasComparator)
	{
		putVarint32(dst,kComparator);
		putLengthPrefixedSlice(dst,comparator);
	}

	if (hasLogNumber)
	{
		putVarint32(dst,kLogNumber);
		putVarint64(dst,logNumber);
	}

	if (hasPrevLogNumber)
	{
		putVarint32(dst,kPrevLogNumber);
		putVarint64(dst,prevLogNumber);
	}

	if (hasNextFileNumber)
	{
		putVarint32(dst,kNextFileNumber);
		putVarint64(dst,nextFileNumber);
	}

	if (hasLastSequence)
	{
		putVarint32(dst,kLastSequence);
		putVarint64(dst,lastSequence);
	}

	for (size_t i = 0; i < compactPointers.size(); i++)
	{
		putVarint32(dst,kCompactPointer);
		putVarint32(dst,compactPointers[i].first);  // level
		putLengthPrefixedSlice(dst,compactPointers[i].second.encode());
	}

	for (auto iter = deletedFiles.begin(); iter != deletedFiles.end(); ++iter)
	{
		PutVarint32(dst, kDeletedFile);
		PutVarint32(dst, iter->first);   // level
		PutVarint64(dst, iter->second);  // file number
	}

	for (size_t i = 0; i < newFiles.size(); i++)
	{
		const FileMetaData &f = newFiles[i].second;
		putVarint32(dst,kNewFile);
		putVarint32(dst,newFiles[i].first);  // level
		putVarint64(dst,f.number);
		putVarint64(dst,f.file_size);
		putLengthPrefixedSlice(dst,f.smallest.encode());
		putLengthPrefixedSlice(dst,f.largest.encode());
	}
}

static bool getInternalKey(std::string_view *input,InternalKey *dst)
{
	std::string_view str;
	if (getLengthPrefixedSlice(input,&str))
	{
		dst->decodeFrom(str);
		return true;
	}
	else
	{
		return false;
	}
}

static bool getLevel(std::string_view *input,int *level)
{
	uint32_t v;
	if (getVarint32(input,&v) && v < config::kNumLevels)
	{
		*level = v;
		return true;
	}
	else
	{
		return false;
	}
}

Status VersionEdit::decodeFrom(const std::string_view &src)
{
	clear();
	std::string_view input = src;
	const char *msg = nullptr;
	uint32_t tag;

	// Temporary storage for parsing
	int level;
	uint64_t number;
	FileMetaData f;
	std::string_view str;
	InternalKey key;

	while (msg == nullptr && getVarint32(&input,&tag))
	{
		switch (tag)
		{
			case kComparator:
			if (getLengthPrefixedSlice(&input,&str))
			{
				comparator = std::string(str.data(),str.size());
				hasComparator = true;
			}
			else
			{
				msg = "comparator name";
			}
			break;

		  case kLogNumber:
			if (getVarint64(&input,&logNumber))
			{
				hasLogNumber = true;
			}
			else
			{
				msg = "log number";
			}
			break;

		  case kPrevLogNumber:
			if (getVarint64(&input,&prevLogNumber))
			{
				hasPrevLogNumber = true;
			}
			else
			{
				msg = "previous log number";
			}
			break;

		  case kNextFileNumber:
			if (getVarint64(&input,&nextFileNumber))
			{
				hasNextFileNumber_ = true;
			}
			else
			{
				msg = "next file number";
			}
			break;

		  case kLastSequence:
			if (getVarint64(&input,&lastSequence))
			{
				hasLastSequence = true;
			}
			else
			{
				msg = "last sequence number";
			}
			break;

		  case kCompactPointer:
			if (getLevel(&input,&level) && getInternalKey(&input,&key))
			{
				compactPointers.push_back(std::make_pair(level,key));
			}
			else
			{
				msg = "compaction pointer";
			}
			break;

		  case kDeletedFile:
			if (getLevel(&input,&level) && getVarint64(&input,&number))
			{
				deletedFiles.insert(std::make_pair(level,number));
			}
			else
			{
				msg = "deleted file";
			}
			break;

		  case kNewFile:
			if (getLevel(&input,&level) &&
				getVarint64(&input,&f.number) &&
				getVarint64(&input,&f.fileSize) &&
				getInternalKey(&input,&f.smallest) &&
				getInternalKey(&input,&f.largest))
			{
				newFiles.push_back(std::make_pair(level,f));
			}
			else
			{
				msg = "new-file entry";
			}
			break;

		  default:
			msg = "unknown tag";
			break;
		}
	}

	if (msg == nullptr && !input.empty())
	{
		msg = "invalid tag";
	}

	Status result;
	if (msg != nullptr)
	{
		result = Status::corruption("VersionEdit",msg);
	}
	return result;
}

std::string VersionEdit::debugString() const
{
	std::string r;
	r.append("VersionEdit {");
	if (hasComparator)
	{
		r.append("\n  Comparator: ");
		r.append(comparator);
	}

	if (hasLogNumber)
	{
		r.append("\n  LogNumber: ");
		appendNumberTo(&r,logNumber);
	}

	if (hasPrevLogNumber)
	{
		r.append("\n  PrevLogNumber: ");
		appendNumberTo(&r,prevLogNumber);
	}

	if (hasNextFileNumber)
	{
		r.append("\n  NextFile: ");
		appendNumberTo(&r,nextFileNumber);
	}

	if (hasLastSequence)
	{
		r.append("\n  LastSeq: ");
		appendNumberTo(&r,lastSequence);
	}

	for (size_t i = 0; i < compactPointers.size(); i++)
	{
		r.append("\n  CompactPointer: ");
		appendNumberTo(&r,compactPointers[i].first);
		r.append(" ");
		r.append(compactPointers[i].second.debugString());
	}

	for (auto iter = deletedFiles.begin(); iter != deletedFiles.end(); ++iter)
	{
		r.append("\n  DeleteFile: ");
		appendNumberTo(&r,iter->first);
		r.append(" ");
		appendNumberTo(&r,iter->second);
	}

	for (size_t i = 0; i < newFiles.size(); i++)
	{
		const FileMetaData &f = newFiles[i].second;
		r.append("\n  AddFile: ");
		appendNumberTo(&r,newFiles[i].first);
		r.append(" ");
		appendNumberTo(&r,f.number);
		r.append(" ");
		appendNumberTo(&r,f.fileSize);
		r.append(" ");
		r.append(f.smallest.debugString());
		r.append(" .. ");
		r.append(f.largest.debugString());
	}
	r.append("\n}\n");
	return r;
}




