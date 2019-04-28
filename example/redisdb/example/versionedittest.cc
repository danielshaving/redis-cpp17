#pragma once

#include "versionedit.h"

class VersionEditTest {
public:
    void testEncodeDecode(const VersionEdit &edit) {
        std::string encoded, encoded2;
        edit.encodeTo(&encoded);
        VersionEdit parsed;
        Status s = parsed.decodeFrom(encoded);
        assert(s.ok());
        parsed.encodeTo(&encoded2);
        assert(encoded == encoded2);
    }

    void encodeDecode() {
        static const uint64_t kBig = 1ull << 50;
        VersionEdit edit;

        for (int i = 0; i < 4; i++) {
            testEncodeDecode(edit);
            edit.addFile(3, kBig + 300 + i, kBig + 400 + i,
                         InternalKey("foo", kBig + 500 + i, kTypeValue),
                         InternalKey("zoo", kBig + 600 + i, kTypeDeletion));
            edit.deleteFile(4, kBig + 700 + i);
            edit.setCompactPointer(i, InternalKey("x", kBig + 900 + i, kTypeValue));
        }

        edit.setComparatorName("foo");
        edit.setLogNumber(kBig + 100);
        edit.setNextFile(kBig + 200);
        edit.setLastSequence(kBig + 1000);
        testEncodeDecode(edit);
    }
};

/*
int main()
{
	VersionEditTest vtest;
	vtest.encodeDecode();
	return 0;
}
*/
