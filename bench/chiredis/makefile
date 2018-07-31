TARGET=./redis-server
CFLAGS := -Wall -g -w -O0 -Wunused-function # -I /usr/local/include/  #-DUSE_JEMALLOC
cppfiles := $(shell ls *.c)
cfiles := $(-shell ls *.c)
OBJS := $(patsubst %.c,./%.o, $(cppfiles) $(cfiles))
COBJS=${patsubst %.c,./%.o,$(cfiles)}
CXXOBJS=${patsubst %.c,./%.o,$(cppfiles)}

DEPS=$(patsubst %.o,%.d,$(OBJS))

LIB=  #/usr/local/lib/libprotobuf.a #./libjemalloc.a # ./libhiredis.a    /usr/lib64/libprofiler.so.0 #-ltcmalloc

SO_LIB= 


.PHONY: all clean

all: ${TARGET}

${TARGET}: ${OBJS} ${LIB} 
	gcc -Wunused-function -o $@ $^ ${LDFLAGS}${LIB} ${LIB64}
${CXXOBJS}:./%.o:./%.c
	gcc -MMD -c -o $@ $< ${CFLAGS} 

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
	@echo cfiels=$(cfiles)
	@echo DEPS=$(DEPS)
	@echo CXXOBJS=$(CXXOBJS)
	@echo COBJS=$(COBJS)

