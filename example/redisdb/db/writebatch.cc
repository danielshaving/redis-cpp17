#include "writebatch.h"
#include "coding.h"
#include "util.h"

// WriteBatch header has an 8-byte sequence number followed by a 4-byte Count.
static const size_t kHeader = 12;

WriteBatch::WriteBatch() {
	clear();
}

WriteBatch::~WriteBatch() {}

void WriteBatch::clear() {
	rep.clear();
	rep.resize(kHeader);
}

void WriteBatch::Delete(const std::string_view& key) {
	WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
	rep.push_back(static_cast<char>(kTypeDeletion));
	PutLengthPrefixedSlice(&rep, key);
}

void WriteBatch::Put(const std::string_view& key, const std::string_view& value) {
	WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
	rep.push_back(static_cast<char>(kTypeValue));
	PutLengthPrefixedSlice(&rep, key);
	PutLengthPrefixedSlice(&rep, value);
}

size_t WriteBatch::ApproximateSize() {
	return rep.size();
}

Status WriteBatch::Iterate(uint64_t sequence, const std::shared_ptr<MemTable>& mem) const {
	std::string_view input(rep);
	if (input.size()< kHeader) {
		return Status::Corruption("malformed WriteBatch (too small)");
	}

	input.remove_prefix(kHeader);
	std::string_view key, value;
	int found = 0;
	while (!input.empty()) {
		found++;
		char tag = input[0];
		input.remove_prefix(1);
		switch (tag) {
		case kTypeValue: {
			if (GetLengthPrefixedSlice(&input, &key) && GetLengthPrefixedSlice(&input, &value)) {
				mem->Add(sequence++, kTypeValue, key, value);
			}
			else {
				return Status::Corruption("bad WriteBatch Put");
			}
			break;
		}
		case kTypeDeletion: {
			if (GetLengthPrefixedSlice(&input, &key)) {
				mem->Add(sequence++, kTypeDeletion, key, std::string_view());
			}
			else {
				return Status::Corruption("bad WriteBatch Delete");
			}
			break;
		}
		default:
			return Status::Corruption("unknown WriteBatch tag");
		}
	}

	if (found != WriteBatchInternal::Count(this)) {
		return Status::Corruption("WriteBatch has wrong Count");
	}
	else {
		Status s;
		return s;
	}
}

Status WriteBatchInternal::InsertInto(const WriteBatch* batch, const std::shared_ptr<MemTable>& memtable) {
	return batch->Iterate(GetSequence(batch), memtable);
}

int WriteBatchInternal::Count(const WriteBatch* b) {
	return DecodeFixed32(b->rep.data() + 8);
}

void WriteBatchInternal::SetCount(WriteBatch* b, int n) {
	EncodeFixed32(&b->rep[8], n);
}

uint64_t WriteBatchInternal::GetSequence(const WriteBatch* b) {
	return uint64_t(DecodeFixed64(b->rep.data()));
}

void WriteBatchInternal::SetSequence(WriteBatch* b, uint64_t seq) {
	EncodeFixed64(&b->rep[0], seq);
}

void WriteBatchInternal::SetContents(WriteBatch* b, const std::string_view& Contents) {
	assert(Contents.size() >= kHeader);
	b->rep.assign(Contents.data(), Contents.size());
}

void WriteBatchInternal::append(WriteBatch* dst, const WriteBatch* src) {
	SetCount(dst, Count(dst) + Count(src));
	assert(src->rep.size() >= kHeader);
	dst->rep.append(src->rep.data() + kHeader, src->rep.size() - kHeader);
}


