#include "version-set.h"

VersionSet::VersionSet(const std::string &dbname,const Options *options)
{
	lastSequence = 0;
	nextFileNumber = 1;
}

VersionSet::~VersionSet()
{

}
