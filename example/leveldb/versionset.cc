#include "versionset.h"

VersionSet::VersionSet(const std::string &dbname,const Options *options)
{
	lastSequence = 0;
	nextFileNumber = 0;
}

VersionSet::~VersionSet()
{

}
