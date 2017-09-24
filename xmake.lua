-- set project
set_project("xredis")

-- set warning: -Wall -w
set_warnings("none")

-- set lanuage: c++11
set_languages("cxx11")

-- add c++ flags
add_cxxflags("-Wno-unused", "-Wno-sign-compare", "-Wno-deprecated-declarations", "-Wno-deprecated", "-Wl,--no-as-needed")

-- set the object files directory
set_objectdir("$(buildir)/.objs")

-- includes
includes("src", "bench")
