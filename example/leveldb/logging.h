#pragma once
#include <stdio.h>
#include <stdint.h>
#include <string>
#include <string_view>


// Parse a human-readable number from "*in" into *value.  On success,
// advances "*in" past the consumed number and sets "*val" to the
// numeric value.  Otherwise, returns false and leaves *in in an
// unspecified state.
bool consumeDecimalNumber(std::string_view *in,uint64_t* val);

// Return a human-readable printout of "num"
std::string numberToString(uint64_t num);

// Append a human-readable printout of "num" to *str
void appendNumberTo(std::string *str,uint64_t num);
