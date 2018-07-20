#include "version-set.h"
#include "logging.h"

VersionSet::VersionSet(const std::string &dbname,const Options *options)
{
	lastSequence = 0;
	nextFileNumber = 1;
}

VersionSet::~VersionSet()
{

}

Status VersionSet::recover(bool *manifest)
{
	// Read "CURRENT" file, which contains a pointer to the current manifest file
	std::string current;
	Status s = readFileToString(options->env,currentFileName(dbname),&current);
	if (!s.ok()) 
	{
		return s;
	}

	if (current.empty() || current[current.size() - 1] != '\n') 
	{
		return Status::corruption("CURRENT file does not end with newline");
	}
	current.resize(current.size() - 1);

	std::string dscname = dbname + "/" + current;
	std::shared_ptr<PosixSequentialFile> file;
	s = options->env->newSequentialFile(dscname,file);
	if (!s.ok()) 
	{
		if (s.IsNotFound()) 
		{
			return Status::corruption(
				"CURRENT points to a non-existent file", s.ToString());
		}
		return s;
	}

	bool have
}
