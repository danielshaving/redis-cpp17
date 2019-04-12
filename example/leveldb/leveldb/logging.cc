#include "logging.h"
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits>


bool consumeDecimalNumber(std::string_view *in, uint64_t *val) {
    // Constants that will be optimized away.
	constexpr const uint64_t kMaxUint64 = (std::numeric_limits<uint64_t>::max)();
    constexpr const char kLastDigitOfMaxUint64 = '0' + static_cast<char>(kMaxUint64 % 10);

    uint64_t value = 0;

    // reinterpret_cast-ing from char* to unsigned char* to avoid signedness.
    const unsigned char *start = reinterpret_cast<const unsigned char *>(in->data());
    const unsigned char *end = start + in->size();
    const unsigned char *current = start;
    for (; current != end; ++current) {
        const unsigned char ch = *current;
        if (ch < '0' || ch > '9')
            break;

        // Overflow check.
        // kMaxUint64 / 10 is also constant and will be optimized away.
        if (value > kMaxUint64 / 10 ||
            (value == kMaxUint64 / 10 && ch > kLastDigitOfMaxUint64)) {
            return false;
        }
        value = (value * 10) + (ch - '0');
    }

    *val = value;
    const size_t digitsConsumed = current - start;
    in->remove_prefix(digitsConsumed);
    return digitsConsumed != 0;
}


std::string numberToString(uint64_t num) {
    std::string r;
    appendNumberTo(&r, num);
    return r;
}

void appendNumberTo(std::string *str, uint64_t num) {
    char buf[30];
    snprintf(buf, sizeof(buf), "%llu", (unsigned long long) num);
    str->append(buf);
}

static Status doWriteStringToFile(const std::shared_ptr <Env> &env, const std::string_view &data,
                                  const std::string &fname, bool shouldSync) {
    std::shared_ptr <WritableFile> file;
    Status s = env->newWritableFile(fname, file);
    if (!s.ok()) {
        return s;
    }

    s = file->append(data);
    if (s.ok() && shouldSync) {
        s = file->sync();
    }

    if (s.ok()) {
        s = file->close();
    }

    if (!s.ok()) {
        env->deleteFile(fname);
    }
    return s;
}

// A utility routine: write "data" to the named file.
Status writeStringToFile(const std::shared_ptr <Env> &env, const std::string_view &data, const std::string &fname) {
    return doWriteStringToFile(env, data, fname, false);
}

Status writeStringToFileSync(const std::shared_ptr <Env> &env, const std::string &data, const std::string &fname) {
    return doWriteStringToFile(env, data, fname, true);
}

// A utility routine: read contents of named file into *data
Status readFileToString(const std::shared_ptr <Env> &env, const std::string &fname, std::string *data) {
    data->clear();
    std::shared_ptr <SequentialFile> file;
    Status s = env->newSequentialFile(fname, file);
    if (!s.ok()) {
        return s;
    }
    static const int kBufferSize = 8192;
    char *space = (char *) malloc(kBufferSize);
    while (true) {
        std::string_view fragment;
        s = file->read(kBufferSize, &fragment, space);
        if (!s.ok()) {
            break;
        }

        data->append(fragment.data(), fragment.size());
        if (fragment.empty()) {
            break;
        }
    }

    free(space);
    return s;
}

void appendEscapedStringTo(std::string *str, const std::string_view &value) {
    for (size_t i = 0; i < value.size(); i++) {
        char c = value[i];
        if (c >= ' ' && c <= '~') {
            str->push_back(c);
        } else {
            char buf[10];
            snprintf(buf, sizeof(buf), "\\x%02x",
                     static_cast<unsigned int>(c) & 0xff);
            str->append(buf);
        }
    }
}

std::string escapeString(const std::string &value) {
    std::string r;
    appendEscapedStringTo(&r, value);
    return r;
}
