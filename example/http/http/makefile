TARGET= ./server
CFLAGS := -Wall -w  -g -O0 -Wno-unused -Wno-sign-compare -Wno-deprecated-declarations -Wno-deprecated -Wl,--no-as-needed -std=c++11  -I ../  -I ../../../src/
cppfiles := $(shell ls *.cpp)
#cfiles := $(shell ls *.c)
SRCS := $(wildcard *.cpp ../*cpp) 
OBJS := $(patsubst %.cpp,./%.o, $(cppfiles) $(cfiles) $(SRCS))
COBJS=${patsubst %.c,./%.o,$(cfiles)}
CXXOBJS=${patsubst %.cpp,./%.o,$(cppfiles)}
DEPS=$(patsubst %.o,$(OBJS))

LIB= ../../../lib/libxredis.a  

SO_LIB= 


.PHONY: all clean

all: ${TARGET}

${TARGET}: ${OBJS} ${LIB} 
	${CXX} -o $@ $^ ${LDFLAGS}${LIB} ${LIB64}  -lz -lpthread 

${CXXOBJS}:./%.o:./%.cpp
	${CXX} -MMD -c -o $@ $< ${CFLAGS} 

${COBJS}:./%.o:./%.c
	${CC} -MMD -c -o $@ $< ${CFLAGS} 

clean:
	rm -f *.d *.rdb *.log *.temp ${OBJS} ${TARGET}

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

