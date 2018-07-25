#include "dbimpl.h"
#include "option.h"

int main(int argc,char *argv[])
{
	Options options;
	options.createIfMissing = true;
	DBImpl db(options,"db");
	Status s = db.open();
	if(!s.ok())
	{
		printf("%s\n",s.toString().c_str());
	}

	for(int i = 0 ; i < 100; i++)
	{
		std::string key = std::to_string(i);
		std::string value = std::to_string(i);
		//std::string v;
		s = db.put(WriteOptions(),key,value);
		assert(s.ok());
		//s = db.get(ReadOptions(),key,&v);
		//assert(s.ok());
	}

    for(int i = 101 ; i < 1010; i++)
    {
        std::string key = std::to_string(i);
        std::string v;
        s = db.get(ReadOptions(),key,&v);
        assert(s.ok());
    }

	return 0;
}
