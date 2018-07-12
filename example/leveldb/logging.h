#pragma once
#include <stdio.h>
#include <stdint.h>
#include <string>
#include <string_view>

bool consumeDecimalNumber(std::string_view *in,uint64_t* val);
