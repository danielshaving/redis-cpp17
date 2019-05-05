#include "option.h"

// Create an Options object with default values for all fields.

static BytewiseComparatorImpl byteImpl;

Options::Options()
	: comparator(&byteImpl),
	createifmissing(false),
	errorifexists(false),
	paranoidchecks(false),
	writebuffersize(4 << 20),
	maxopenfiles(1000),
	blocksize(4096),
	blockrestartinterval(16),
	maxfilesize(2 << 20),
	compression(kNoCompression),
	reuselogs(false),
	env(new Env()) {

}
