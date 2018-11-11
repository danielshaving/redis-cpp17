#pragma once
#include <stdio.h>
#include <stdint.h>
#include <string>
#include <string_view>
#include "posix.h"
#include "status.h"

// Parse a human-readable number from "*in" into *value.  On success,
// advances "*in" past the consumed number and sets "*val" to the
// numeric value.  Otherwise, returns false and leaves *in in an
// unspecified state.
bool consumeDecimalNumber(std::string_view *in, uint64_t *val);

// Return a human-readable printout of "num"
std::string numberToString(uint64_t num);

// Append a human-readable printout of "num" to *str
void appendNumberTo(std::string *str, uint64_t num);

// A utility routine: write "data" to the named file.
Status writeStringToFile(PosixEnv *env, const std::string_view &data, const std::string &fname);

// A utility routine: read contents of named file into *data
Status readFileToString(PosixEnv *env, const std::string &fname, std::string *data);

Status writeStringToFileSync(PosixEnv *env, const std::string &data, const std::string &fname);

std::string escapeString(const std::string &value);
