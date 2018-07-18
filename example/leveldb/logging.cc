#include "logging.h"

bool consumeDecimalNumber(std::string_view *in,uint64_t* val)
{
	// Constants that will be optimized away.
	constexpr const uint64_t kMaxUint64 = std::numeric_limits<uint64_t>::max();
	constexpr const char kLastDigitOfMaxUint64 = '0' + static_cast<char>(kMaxUint64 % 10);

	uint64_t value = 0;

	// reinterpret_cast-ing from char* to unsigned char* to avoid signedness.
	const unsigned char *start = reinterpret_cast<const unsigned char*>(in->data());
	const unsigned char *end = start + in->size();
	const unsigned char *current = start;
	for (; current != end; ++current)
	{
		const unsigned char ch = *current;
		if (ch < '0' || ch > '9')
			break;

		// Overflow check.
		// kMaxUint64 / 10 is also constant and will be optimized away.
		if (value > kMaxUint64 / 10 ||
			(value == kMaxUint64 / 10 && ch > kLastDigitOfMaxUint64))
		{
			return false;
		}

		value = (value * 10) + (ch - '0');
	}

	*val = value;
	const size_t digitsConsumed = current - start;
	in->remove_prefix(digitsConsumed);
	return digitsConsumed != 0;
}

std::string numberToString(uint64_t num)
{
	std::string r;
	appendNumberTo(&r,num);
	return r;
}

void appendNumberTo(std::string *str,uint64_t num)
{
	char buf[30];
	snprintf(buf, sizeof(buf), "%llu", (unsigned long long) num);
	str->append(buf);
}

