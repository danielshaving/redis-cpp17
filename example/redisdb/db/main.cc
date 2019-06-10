#include "redisdb.h"

bool CHECK_MATCH_RESULT(RedisDB *const db, const std::string_view& key,
	int32_t expectsize) {
	int32_t size = 0;
	Status s = db->ZCard(key, &size);
	if (!s.ok() && !s.IsNotFound()) {
		return false;
	}

	if (s.IsNotFound() && !expectsize) {
		return true;
	}
	return size == expectsize;
}

bool CHECK_MAKR_EXPIRED(RedisDB *const db,
                         const std::string_view& key) {
	std::map<DataType, Status> typestatus;
	int ret = db->Expire(key, 1, &typestatus);
	if (!ret || !typestatus[DataType::kZSets].ok()) {
		return false;
	}
	std::this_thread::sleep_for(std::chrono::milliseconds(2000));
	return true;
}

bool CHECK_MEMBER_MATCH_RESULT(RedisDB *const db,
                                const std::string_view& key,
                                const std::vector<ScoreMember>& expectsm) {
	std::vector<ScoreMember> smout;
	Status s = db->ZRange(key, 0, -1, &smout);
	if (!s.ok() && !s.IsNotFound()) {
		return false;
	}

	if (smout.size() != expectsm.size()) {
		return false;
	}

	if (s.IsNotFound() && expectsm.empty()) {
		return true;
	}

	for (int idx = 0; idx < smout.size(); ++idx) {
		if (expectsm[idx].score != smout[idx].score
			|| expectsm[idx].member != smout[idx].member) {
			return false;
		}
	}
	return true;
}

bool DELETE_KEY(RedisDB *const db,
                       const std::string_view& key) {
	std::vector<std::string> delkeys = {ToString(key)};
	std::map<DataType, Status> typestatus;
	db->Del(delkeys, &typestatus);
	return typestatus[DataType::kZSets].ok();
}

int main() {
	Options opts;
	opts.createifmissing = true;
	RedisDB db(opts, "./test_sortset");
	Status s = db.Open();
	if (s.ok()) {
		printf("Open success\n");
	}
	else {
		printf("Open failed, error: %s\n", s.ToString().c_str());
		return -1;
	}
	
	int32_t ret;	
	std::map<DataType, int64_t> typettl;
	std::map<DataType, Status> typestatus;

	std::vector<ScoreMember> sm1{{3.23, "MM1"}, {0, "MM2"}, {8.0004, "MM3"}, {-0.54, "MM4"}};
	s = db.ZAdd("TEST1_ZADD_KEY", sm1, &ret);
	assert(s.ok());
	assert(4 == ret);
	assert(CHECK_MATCH_RESULT(&db, "TEST1_ZADD_KEY", 4));
	assert(CHECK_MEMBER_MATCH_RESULT(&db, "TEST1_ZADD_KEY", 
		{{-0.54, "MM4"}, {0, "MM2"}, {3.23, "MM1"}, {8.0004, "MM3"}}));

	std::vector<ScoreMember> sm2{{0, "MM1"}, {0, "MM1"}, {0, "MM2"}, {0, "MM3"}};
	s = db.ZAdd("TEST2_ZADD_KEY", sm2, &ret);
	assert(s.ok());
	assert(3 == ret);
	assert(CHECK_MATCH_RESULT(&db, "TEST2_ZADD_KEY", 3));
	assert(CHECK_MEMBER_MATCH_RESULT(&db, "TEST2_ZADD_KEY", {{0, "MM1"}, {0, "MM2"}, {0, "MM3"}}));

	std::vector<ScoreMember> sm3{{1/1.0, "MM1"}, {1/3.0, "MM2"}, {1/6.0, "MM3"}, {1/7.0, "MM4"}};
	s = db.ZAdd("TEST3_ZADD_KEY", sm3, &ret);
	assert(s.ok());
	assert(4 == ret);
	assert(CHECK_MATCH_RESULT(&db, "TEST3_ZADD_KEY", 4));
	assert(CHECK_MEMBER_MATCH_RESULT(&db, "TEST3_ZADD_KEY", 
		{{1/7.0, "MM4"}, {1/6.0, "MM3"}, {1/3.0, "MM2"}, {1/1.0, "MM1"}}));

	std::vector<ScoreMember> sm4{{-1/1.0, "MM1"}, {-1/3.0, "MM2"}, {-1/6.0, "MM3"}, {-1/7.0, "MM4"}};
	s = db.ZAdd("TEST4_ZADD_KEY", sm4, &ret);
	assert(s.ok());	
	assert(4 ==  ret);
	assert(CHECK_MATCH_RESULT(&db, "TEST4_ZADD_KEY", 4));
	assert(CHECK_MEMBER_MATCH_RESULT(&db, "TEST4_ZADD_KEY", 
		{{-1/1.0, "MM1"}, {-1/3.0, "MM2"}, {-1/6.0, "MM3"}, {-1/7.0, "MM4"}}));

	s = db.ZAdd("TEST5_ZADD_KEY", {{0, "MM1"}}, &ret);
	assert(s.ok());
	assert(1 == ret);
	assert(CHECK_MATCH_RESULT(&db, "TEST5_ZADD_KEY", 1));
	assert(CHECK_MEMBER_MATCH_RESULT(&db, "TEST5_ZADD_KEY", {{0, "MM1"}}));

	s = db.ZAdd("TEST5_ZADD_KEY", {{-0.5333, "MM2"}}, &ret);
	assert(s.ok());
	assert(1 == ret);
	assert(CHECK_MATCH_RESULT(&db, "TEST5_ZADD_KEY", 2));
	assert(CHECK_MEMBER_MATCH_RESULT(&db, "TEST5_ZADD_KEY", {{-0.5333, "MM2"}, {0, "MM1"}}));

	s = db.ZAdd("TEST5_ZADD_KEY", {{1.79769e+308, "MM3"}}, &ret);
	assert(s.ok());
	assert(1 == ret);
	assert(CHECK_MATCH_RESULT(&db, "TEST5_ZADD_KEY", 3));
	assert(CHECK_MEMBER_MATCH_RESULT(&db, "TEST5_ZADD_KEY",
		{{-0.5333, "MM2"}, {0, "MM1"}, {1.79769e+308, "MM3"}}));

	s = db.ZAdd("TEST5_ZADD_KEY", {{50000, "MM4"}}, &ret);
	assert(s.ok());
	assert(1 == ret);
	assert(CHECK_MATCH_RESULT(&db, "TEST5_ZADD_KEY", 4));
	assert(CHECK_MEMBER_MATCH_RESULT(&db, "TEST5_ZADD_KEY",
		{{-0.5333, "MM2"}, {0, "MM1"}, {50000, "MM4"}, {1.79769e+308, "MM3"}}));

	s = db.ZAdd("TEST5_ZADD_KEY", {{-1.79769e+308, "MM5"}}, &ret);
	assert(s.ok());
	assert(1 == ret);
	assert(CHECK_MATCH_RESULT(&db, "TEST5_ZADD_KEY", 5));
	assert(CHECK_MEMBER_MATCH_RESULT(&db, "TEST5_ZADD_KEY",
		{{-1.79769e+308, "MM5"}, {-0.5333,      "MM2"}, {0, "MM1"},
			{50000,         "MM4"}, {1.79769e+308, "MM3"}}));

	s = db.ZAdd("TEST5_ZADD_KEY", {{0, "MM6"}}, &ret);
	assert(s.ok());
	assert(1 == ret);
	assert(CHECK_MATCH_RESULT(&db, "TEST5_ZADD_KEY", 6));
	assert(CHECK_MEMBER_MATCH_RESULT(&db, "TEST5_ZADD_KEY",
		{{-1.79769e+308, "MM5"}, {-0.5333, "MM2"}, {0,            "MM1"},
			{0,             "MM6"}, {50000,   "MM4"}, {1.79769e+308, "MM3"}}));

	s = db.ZAdd("TEST5_ZADD_KEY", {{100000, "MM6"}}, &ret);
	assert(s.ok());
	assert(0 == ret);
	assert(CHECK_MATCH_RESULT(&db, "TEST5_ZADD_KEY", 6));
	assert(CHECK_MEMBER_MATCH_RESULT(&db, "TEST5_ZADD_KEY",
		{{-1.79769e+308, "MM5"}, {-0.5333, "MM2"}, {0, "MM1"},
			{50000,         "MM4"}, {100000,  "MM6"}, {1.79769e+308, "MM3"}}));

	s = db.ZAdd("TEST5_ZADD_KEY", {{-0.5333, "MM7"}}, &ret);
	assert(s.ok());
	assert(1 == ret);
	assert(CHECK_MATCH_RESULT(&db, "TEST5_ZADD_KEY", 7));
	assert(CHECK_MEMBER_MATCH_RESULT(&db, "TEST5_ZADD_KEY",
		{{-1.79769e+308, "MM5"}, {-0.5333, "MM2"}, {-0.5333, "MM7"},
			{0,             "MM1"}, {50000,   "MM4"}, {100000,  "MM6"},
			{1.79769e+308,  "MM3"}}));

	s = db.ZAdd("TEST5_ZADD_KEY", {{-1/3.0, "MM8"}}, &ret);
	assert(s.ok());
	assert(1 == ret);
	assert(CHECK_MATCH_RESULT(&db, "TEST5_ZADD_KEY", 8));
	assert(CHECK_MEMBER_MATCH_RESULT(&db, "TEST5_ZADD_KEY",
		{{-1.79769e+308, "MM5"}, {-0.5333,      "MM2"}, {-0.5333, "MM7"},
			{-1/3.0,       "MM8"},  {0,            "MM1"}, {50000,   "MM4"},
			{100000,        "MM6"}, {1.79769e+308, "MM3"}}));

	s = db.ZAdd("TEST5_ZADD_KEY", {{1/3.0, "MM9"}}, &ret);
	assert(s.ok());
	assert(1 == ret);
	assert(CHECK_MATCH_RESULT(&db, "TEST5_ZADD_KEY", 9));
	assert(CHECK_MEMBER_MATCH_RESULT(&db, "TEST5_ZADD_KEY",
		{{-1.79769e+308, "MM5"}, {-0.5333, "MM2"}, {-0.5333,      "MM7"},
			{-1/3.0,        "MM8"}, {0,       "MM1"}, {1/3.0,        "MM9"},
			{50000,         "MM4"}, {100000,  "MM6"}, {1.79769e+308, "MM3"}}));

	s = db.ZAdd("TEST5_ZADD_KEY", {{0, "MM1"}, {0, "MM2"}, {0, "MM3"},
								{0, "MM4"}, {0, "MM5"}, {0, "MM6"},
								{0, "MM7"}, {0, "MM8"}, {0, "MM9"}}, &ret);
	assert(s.ok());
	assert(0 == ret);
	assert(CHECK_MATCH_RESULT(&db, "TEST5_ZADD_KEY", 9));
	assert(CHECK_MEMBER_MATCH_RESULT(&db, "TEST5_ZADD_KEY",
		{{0, "MM1"}, {0, "MM2"}, {0, "MM3"},
			{0, "MM4"}, {0, "MM5"}, {0, "MM6"},
			{0, "MM7"}, {0, "MM8"}, {0, "MM9"}}));


	std::vector<ScoreMember> sm6{{-1, "MM1"}, {0, "MM2"}, {1, "MM3"}};
	s = db.ZAdd("TEST6_ZADD_KEY", sm6, &ret);
	assert(s.ok());
	assert(3 == ret);
	assert(CHECK_MATCH_RESULT(&db, "TEST6_ZADD_KEY", 3));
	assert(CHECK_MEMBER_MATCH_RESULT(&db, "TEST6_ZADD_KEY", {{-1, "MM1"}, {0, "MM2"}, {1, "MM3"}}));
	assert(CHECK_MAKR_EXPIRED(&db, "TEST6_ZADD_KEY"));
	assert(CHECK_MATCH_RESULT(&db, "TEST6_ZADD_KEY", 0));
	assert(CHECK_MEMBER_MATCH_RESULT(&db, "TEST6_ZADD_KEY", {}));

	std::vector<ScoreMember> sm6_1{{-100, "MM1"}, {0, "MM2"}, {100, "MM3"}};
	s = db.ZAdd("TEST6_ZADD_KEY", sm6_1, &ret);
	assert(s.ok());
	assert(3 == ret);
	assert(CHECK_MATCH_RESULT(&db, "TEST6_ZADD_KEY", 3));
	assert(CHECK_MEMBER_MATCH_RESULT(&db, "TEST6_ZADD_KEY", {{-100, "MM1"}, {0, "MM2"}, {100, "MM3"}}));

	std::vector<ScoreMember> sm7 {{-0.123456789, "MM1"}, {0, "MM2"}, {0.123456789, "MM3"}};
	s = db.ZAdd("TEST7_ZADD_KEY", sm7, &ret);
	assert(s.ok());
	assert(3 == ret);
	assert(CHECK_MATCH_RESULT(&db, "TEST7_ZADD_KEY", 3));
	assert(CHECK_MEMBER_MATCH_RESULT(&db, "TEST7_ZADD_KEY", {{-0.123456789, "MM1"}, {0, "MM2"}, {0.123456789, "MM3"}}));
	assert(DELETE_KEY(&db, "TEST7_ZADD_KEY"));
	assert(CHECK_MATCH_RESULT(&db, "TEST7_ZADD_KEY", 0));
	assert(CHECK_MEMBER_MATCH_RESULT(&db, "TEST7_ZADD_KEY", {}));

	std::vector<ScoreMember> sm7_1{{-1234.56789, "MM1"}, {0, "MM2"}, {1234.56789, "MM3"}};
	s = db.ZAdd("TEST7_ZADD_KEY", sm7_1, &ret);
	assert(s.ok());
	assert(3 == ret);
	assert(CHECK_MATCH_RESULT(&db, "TEST7_ZADD_KEY", 3));
	assert(CHECK_MEMBER_MATCH_RESULT(&db, "TEST7_ZADD_KEY", {{-1234.56789, "MM1"}, {0, "MM2"}, {1234.56789, "MM3"}}));

	s = db.ZAdd("TEST7_ZADD_KEY", {{1234.56789, "MM1"}}, &ret);
	assert(s.ok());
	assert(0 == ret);
	assert(CHECK_MATCH_RESULT(&db, "TEST7_ZADD_KEY", 3));
	assert(CHECK_MEMBER_MATCH_RESULT(&db, "TEST7_ZADD_KEY", {{0, "MM2"}, {1234.56789, "MM1"}, {1234.56789, "MM3"}}));

	std::vector<ScoreMember> sm8{{1, "MM1"}};
	std::vector<ScoreMember> sm8_1{{2, "MM2"}};
	s = db.ZAdd("TEST8_ZADD_KEY", sm8, &ret);
	assert(s.ok());
	assert(1 == ret);
	assert(CHECK_MATCH_RESULT(&db, "TEST8_ZADD_KEY", 1));
	assert(CHECK_MEMBER_MATCH_RESULT(&db, "TEST8_ZADD_KEY", {{1, "MM1"}}));

	typestatus.clear();
	ret = db.Expire("TEST8_ZADD_KEY", 100, &typestatus);
	assert(ret == 1);
	assert(typestatus[DataType::kZSets].ok());
	return 0;
}