#pragma once

#include <stdio.h>
#include <stdint.h>
#include <string>
#include <string_view>
#include "env.h"
#include "status.h"

// Parse a human-readable number from "*in" into *value.  On success,
// advances "*in" past the consumed number and sets "*val" to the
// numeric value.  Otherwise, returns false and leaves *in in an
// unspecified state.
bool ConsumeDecimalNumber(std::string_view* in, uint64_t* val);

// Return a human-readable printout of "num"
std::string NumberToString(uint64_t num);

// Append a human-readable printout of "num" to *str
void AppendNumberTo(std::string* str, uint64_t num);

// A utility routine: Write "data" to the named file.
Status WriteStringToFile(const std::shared_ptr<Env>& env, const std::string_view& data, const std::string& fname);

// A utility routine: read Contents of named file into *data
Status ReadFileToString(const std::shared_ptr<Env>& env, const std::string& fname, std::string* data);

Status WriteStringToFileSync(const std::shared_ptr<Env>& env, const std::string& data, const std::string& fname);

// Return a human-readable version of "value".
// Escapes any non-printable characters found in "value".
std::string EscapeString(const std::string& value);

