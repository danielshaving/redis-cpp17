TARGET= ./redis-server
CFLAGS := -Wall -w  -g -ggdb -O0 -Wno-unused -Wno-sign-compare -Wno-deprecated-declarations -Wno-deprecated -Wl,--no-as-needed -std=c++17 #-D_BOOST_FILE_LOCK  # -I /usr/local/include/  #-DUSE_JEMALLOC
cppfiles := $(shell ls *.cc)
cfiles := $(-shell ls *.c)
OBJS := $(patsubst %.cc,./%.o, $(cppfiles) $(cfiles))
COBJS=${patsubst %.c,./%.o,$(cfiles)}
CXXOBJS=${patsubst %.cc,./%.o,$(cppfiles)}

DEPS=$(patsubst %.o,%.d,$(OBJS))

LIB=  #/usr/local/lib/libprofiler.a /usr/local/lib/libtcmalloc.a #/usr/local/lib/libprotobuf.a #./libjemalloc.a # ./libhiredis.a    /usr/lib64/libprofiler.so.0 #-ltcmalloc

SO_LIB= 


.PHONY: all clean

all: ${TARGET}

${TARGET}: ${OBJS} ${LIB} 
	g++-7 -o $@ $^ ${LDFLAGS}${LIB} ${LIB64}  -lpthread -lstdc++fs #-lz -lcurl -lcrypto -ldl -lssl 
${CXXOBJS}:./%.o:./%.cc
	g++-7 -MMD -c -o $@ $< ${CFLAGS} 

${COBJS}:./%.o:./%.c
	${CC} -MMD -c -o $@ $< ${CFLAGS} 

-include $(DEPS)

clean:
	rm -rf redislog *.sock *.rdb *.log *.temp ${OBJS} ${TARGET} ${DEPS}

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

