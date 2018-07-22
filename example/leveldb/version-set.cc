#include "version-set.h"
#include "logging.h"
#include "filename.h"
#include "log-reader.h"

VersionSet::VersionSet(const std::string &dbname,const Options &options)
:dbname(dbname),
 options(options),
 lastSequence(0),
 nextFileNumber(0)
{

}

VersionSet::~VersionSet()
{

}

void VersionSet::apply(VersionEdit *edit)
{

}

Status VersionSet::recover(bool *manifest)
{
	// Read "CURRENT" file, which contains a pointer to the current manifest file
	std::string current;
	Status s = readFileToString(options.env,currentFileName(dbname),&current);
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
	s = options.env->newSequentialFile(dscname,file);
	if (!s.ok()) 
	{
		if (s.isNotFound())
		{
			return Status::corruption(
				"CURRENT points to a non-existent file",s.toString());
		}
		return s;
	}

	bool haveLogNumber = false;
	bool havePrevLogNumber = false;
	bool haveNextFile = false;
	bool haveLastSequence = false;

	uint64_t nextFile = 0;
	uint64_t lastSequence = 0;
	uint64_t logNumber = 0;
	uint64_t prevLogNumber = 0;

	std::shared_ptr<Version> version(new Version(this));
	LogReporter reporter;
    reporter.status = &s;

	LogReader reader(file.get(),&reporter,true/*checksum*/,0/*initial_offset*/);
    std::string_view record;
	std::string scratch;
	while (reader.readRecord(&record,&scratch) && s.ok())
	{
		VersionEdit edit;
		s = edit.decodeFrom(record);
        if (s.ok())
        {
            apply(&edit);
        }

        if (edit.hasLogNumber)
        {
            logNumber = edit.logNumber;
            haveLogNumber = true;
        }

        if (edit.hasPrevLogNumber)
        {
            prevLogNumber = edit.prevLogNumber;
            havePrevLogNumber = true;
        }

        if (edit.hasNextFileNumber)
        {
            nextFile = edit.nextFileNumber;
            haveNextFile = true;
        }

        if (edit.hasLastSequence)
        {
            lastSequence = edit.lastSequence;
            haveLastSequence = true;
        }
    }

    if (s.ok())
    {
        if (!haveNextFile)
        {
            s = Status::corruption("no meta-nextfile entry in descriptor");
        }
        else if (!haveLogNumber)
        {
            s = Status::corruption("no meta-lognumber entry in descriptor");
        }
        else if (!haveLastSequence)
        {
            s = Status::corruption("no last-sequence-number entry in descriptor");
        }

        if (!havePrevLogNumber)
        {
            prevLogNumber = 0;
        }

        markFileNumberUsed(prevLogNumber);
        markFileNumberUsed(logNumber);
    }
}

void VersionSet::markFileNumberUsed(uint64_t number)
{
    if (nextFileNumber <= number)
    {
        nextFileNumber = number + 1;
    }
}
