//
// Created by zhanghao on 2018/7/26.
//

#pragma once

#include <stdint.h>
#include "status.h"
#include "option.h"
#include "posix.h"

class TableBuilder
{
public:
    // Create a builder that will store the contents of the table it is
    // building in *file.  Does not close the file.  It is up to the
    // caller to close the file after calling Finish().
    TableBuilder(const Options &options,PosixWritableFile *file);

    TableBuilder(const TableBuilder&) = delete;
    void operator=(const TableBuilder&) = delete;

    ~TableBuilder();

    // Add key,value to the table being constructed.
    // REQUIRES: key is after any previously added key according to comparator.
    // REQUIRES: Finish(), Abandon() have not been called
    void add(const std::string_view &key,const std::string_view &value);



private:
    struct Rep;
    std::shared_ptr<Rep> rep;
};