TARGET= ../build/redis-server

CFLAGS := -Wall -Wno-unused -Wno-sign-compare -g -O0 -Wno-deprecated-declarations -Wno-deprecated -Wl,--no-as-needed -std=c++11	-DUSE_JEMALLOC

cppfiles := $(shell ls *.cpp)
cfiles := $(-shell ls *.c)
OBJS := $(patsubst %.cpp,../obj/%.o, $(cppfiles) $(cfiles))
COBJS=${patsubst %.c,../obj/%.o,$(cfiles)}
CXXOBJS=${patsubst %.cpp,../obj/%.o,$(cppfiles)}

DEPS=$(patsubst %.o,%.d,$(OBJS))

LIB= ../../lib/libjemalloc.a  -lpthread  -lssl -lcrypto -lz -ldl 



#LIB=/usr/libevent/lib/libevent.a /usr/libevent/lib/libevent_pthreads.a ../../Lib_3rd/lib/Engine.a


SO_LIB=

.PHONY: all clean

all: ${TARGET}

${TARGET}: ${OBJS} ${LIB} 
	${CXX} -lrt -o $@ $^ ${LDFLAGS}${LIB} ${LIB64} -lpthread

${CXXOBJS}:../obj/%.o:../src/%.cpp
	${CXX} -MMD -c -o $@ $< ${CFLAGS} 

${COBJS}:../obj/%.o:../src/%.c
	${CC} -MMD -c -o $@ $< ${CFLAGS} 

-include $(DEPS)

clean:
	rm -f ${OBJS} ${TARGET} ${DEPS}

show:
	@echo GPROF=$(GPROF)
	@echo CFLAGS=$(CFLAGS)
	@echo LDFLAGS=$(LDFLAGS)
	@echo objs=$(OBJS)
	@echo cppfiels=$(cppfiles)
	@echo cfiels=$(cfiles)
	@echo DEPS=$(DEPS)
	@echo CXXOBJS=$(CXXOBJS)
	@echo COBJS=$(COBJS)

