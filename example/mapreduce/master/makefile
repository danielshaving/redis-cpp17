TARGET= ./master
CFLAGS := -Wall -w  -g  -ggdb -O3 -Wno-unused -Wno-sign-compare -Wno-deprecated-declarations -Wno-deprecated -Wl,--no-as-needed -std=c++17 \
 -I ../../../src/redis/
ccfiles := $(shell ls *.cc)
cfiles := $(-shell ls *.c)
OBJS := $(patsubst %.cc,./%.o, $(ccfiles) $(cfiles))
COBJS=${patsubst %.c,./%.o,$(cfiles)}
CXXOBJS=${patsubst %.cc,./%.o,$(ccfiles)}

DEPS=$(patsubst %.o,%.d,$(OBJS))

LIB=  ../../../src/redis/redis.a

SO_LIB= 


.PHONY: all clean

all: ${TARGET}

${TARGET}: ${OBJS} ${LIB} 
	${CXX} -o $@ $^ ${LDFLAGS}${LIB} ${LIB64} -lpthread -lstdc++fs #-lz -lcurl -lcrypto -ldl -lssl 

${CXXOBJS}:./%.o:./%.cc
	${CXX} -MMD -c -o $@ $< ${CFLAGS} 

${COBJS}:./%.o:./%.c
	${CC} -MMD -c -o $@ $< ${CFLAGS} 

-include $(DEPS)

clean:
	rm -f *.rdb *.log *.temp ${OBJS} ${TARGET} ${DEPS}

show:
	@echo GPROF=$(GPROF)
	@echo CFLAGS=$(CFLAGS)
	@echo LDFLAGS=$(LDFLAGS)
	@echo objs=$(OBJS)
	@echo ccfiels=$(ccfiles)
	@echo cfiels=$(cfiles)
	@echo DEPS=$(DEPS)
	@echo CXXOBJS=$(CXXOBJS)
	@echo COBJS=$(COBJS)

