#pragma once
#include <string>
#include <mutex>
#include <vector>
#include <algorithm>
#include <memory>
#include <shared_mutex>
#include "db.h"

class LockMgr {
public:
	void Lock(const std::string& key) {
		lockshards[std::hash<std::string>{}(key) % kShards].lock();
	}

	void Unlock(const std::string& key) {
		lockshards[std::hash<std::string>{}(key) % kShards].unlock();
	}

	void LockReadShared(const std::string& key) {
		sharedlockhareds[std::hash<std::string>{}(key) % kShards].lock_shared();
	}

	void UnLockReadShared(const std::string& key) {
		sharedlockhareds[std::hash<std::string>{}(key) % kShards].unlock_shared();
	}

	void LockWriteShared(const std::string& key) {
		sharedlockhareds[std::hash<std::string>{}(key) % kShards].lock();
	}

	void UnLockWriteShared(const std::string& key) {
		sharedlockhareds[std::hash<std::string>{}(key) % kShards].unlock();
	}
	
	const static int32_t kShards = 4096;
	std::array<std::mutex, kShards> lockshards;
	std::array<std::shared_mutex, kShards> sharedlockhareds;
};

class HashLock {
public:
	HashLock(LockMgr* lockmgr, const std::string_view& key)
		:lockmgr(lockmgr),
		key(key) {
		lockmgr->Lock(ToString(key));
	}

	~HashLock() {
		lockmgr->Unlock(ToString(key));
	}
private:
	std::string_view key;
	LockMgr* const lockmgr;

	HashLock(const HashLock&);
	void operator=(const HashLock&);
};

class ReadSharedHashLock {
public:
	ReadSharedHashLock(LockMgr* lockmgr, const std::string_view& key)
		:lockmgr(lockmgr),
		key(key) {
		lockmgr->LockReadShared(ToString(key));
	}

	~ReadSharedHashLock() {
		lockmgr->UnLockReadShared(ToString(key));
	}
private:
	std::string_view key;
	LockMgr* const lockmgr;

	ReadSharedHashLock(const ReadSharedHashLock&);
	void operator=(const ReadSharedHashLock&);
};

class WriteSharedHashLock {
public:
	WriteSharedHashLock(LockMgr* lockmgr, const std::string_view& key)
		:lockmgr(lockmgr),
		key(key) {
		lockmgr->LockWriteShared(ToString(key));
	}

	~WriteSharedHashLock() {
		lockmgr->UnLockWriteShared(ToString(key));
	}
private:
	std::string_view key;
	LockMgr* const lockmgr;

	WriteSharedHashLock(const WriteSharedHashLock&);
	void operator=(const WriteSharedHashLock&);
};

class MultiHashLock {
public:
	MultiHashLock(LockMgr* lockmgr, std::vector<std::string>& keys)
		:lockmgr(lockmgr),
		keys(keys) {
		std::string prekey;
		std::sort(keys.begin(), keys.end());
		if (!keys.empty() &&
			keys[0].empty()) {
			lockmgr->Lock(prekey);
		}

		for (const auto& key : keys) {
			if (prekey != key) {
				lockmgr->Lock(key);
				prekey = key;
			}
		}
	}

	~MultiHashLock() {
		std::string prekey;
		if (!keys.empty() &&
			keys[0].empty()) {
			lockmgr->Unlock(prekey);
		}

		for (const auto& key : keys) {
			if (prekey != key) {
				lockmgr->Unlock(key);
				prekey = key;
			}
		}
	}
private:
	std::vector<std::string> keys;
	LockMgr* const lockmgr;

	MultiHashLock(const MultiHashLock&);
	void operator=(const MultiHashLock&);
};

class SnapshotLock {
public:
	SnapshotLock(std::shared_ptr<DB>& db, std::shared_ptr<Snapshot>& snapshot)
		:db(db),
		snapshot(snapshot) {
		snapshot = db->GetSnapshot();
	}

	~SnapshotLock() {
		db->ReleaseSnapshot(snapshot);
	}
private:
	const std::shared_ptr<DB> db;
	const std::shared_ptr<Snapshot> snapshot;

	SnapshotLock(const SnapshotLock&);
	void operator=(const SnapshotLock&);
};
