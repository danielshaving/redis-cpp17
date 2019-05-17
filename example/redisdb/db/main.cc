#include "redisdb.h"

int main() {
	Options opts;
	opts.createifmissing = true;
	RedisDB db(opts, "./db");
	Status s = db.Open();
	if (s.ok()) {
		printf("Open success\n");
	}
	else {
		printf("Open failed, error: %s\n", s.toString().c_str());
		return -1;
	}

	int32_t ret;
	// Set
	s = db.Set("TEST_KEY", "TEST_VALUE");
	printf("Set return: %s\n", s.toString().c_str());

	s = db.Set("TEST_KEY", "TEST_VALUE");
	printf("Set return: %s\n", s.toString().c_str());

	// Get
	std::string value;
	s = db.Get("TEST_KEY", &value);
	printf("Get return: %s, value: %s\n", s.toString().c_str(), value.c_str());
	// GetSet
	s = db.GetSet("TEST_KEY", "Hello", &value);
	printf("GetSet return: %s, oldvalue: %s\n",
		s.toString().c_str(), value.c_str());

	// SetBit
	s = db.SetBit("SETBIT_KEY", 7, 1, &ret);
	printf("Setbit return: %s\n", s.toString().c_str());

	// GetBit
	s = db.GetBit("SETBIT_KEY", 7, &ret);
	printf("GetBit return: %s, ret: %d\n",
		s.toString().c_str(), ret);

	// MSet
	std::vector<KeyValue> kvs;
	kvs.push_back({ "TEST_KEY1", "TEST_VALUE1" });
	kvs.push_back({ "TEST_KEY2", "TEST_VALUE2" });
	s = db.Mset(kvs);
	printf("MSet return: %s\n", s.toString().c_str());

	// MGet
	std::vector<ValueStatus> values;
	std::vector<std::string> keys{ "TEST_KEY1",
	  "TEST_KEY2", "TEST_KEY_NOT_EXIST" };
	s = db.Mget(keys, &values);
	printf("MGet return: %s\n", s.toString().c_str());
	for (size_t idx = 0; idx != keys.size(); idx++) {
		printf("idx = %d, keys = %s, value = %s\n",
			idx, keys[idx].c_str(), values[idx].value.c_str());
	}

	// HSet
	int32_t res;
	s = db.Hset("TEST_KEY1", "TEST_FIELD1", "TEST_VALUE1", &res);
	printf("HSet return: %s, res = %d\n", s.toString().c_str(), res);
	s = db.Hset("TEST_KEY1", "TEST_FIELD2", "TEST_VALUE2", &res);
	printf("HSet return: %s, res = %d\n", s.toString().c_str(), res);

	std::vector<FieldValue> fvs;
	s = db.Hgetall("TEST_KEY1", &fvs);

	return 0;
}

