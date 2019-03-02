#include "option.h"

// Create an Options object with default values for all fields.

static BytewiseComparatorImpl byteImpl;
Options::Options()
	:comparator(&byteImpl),
	createIfMissing(false),
	errorIfExists(false),
	paranoidChecks(false),
	env(new PosixEnv()),
	writeBufferSize(4 << 20),
	maxOpenFiles(1000),
	blockSize(4096),
	blockRestartInterval(16),
	maxFileSize(2 << 20),
	compression(kNoCompression),
	reuseLogs(false)
{

}	