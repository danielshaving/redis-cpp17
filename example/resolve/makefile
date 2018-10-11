TARGET= ./resolve
CFLAGS := -Wall -w  -g -ggdb -O0 -Wno-unused -Wno-sign-compare -Wno-deprecated-declarations  -Wl,--no-as-needed -std=c++17 -I /usr/local/mysql/connector-c++-8.0/include
cppfiles := $(shell ls *.cc)
cfiles := $(-shell ls *.c)
OBJS := $(patsubst %.cc,./%.o, $(cppfiles) $(cfiles))
COBJS=${patsubst %.c,./%.o,$(cfiles)}
CXXOBJS=${patsubst %.cc,./%.o,$(cppfiles)}

DEPS=$(patsubst %.o,%.d,$(OBJS))

LIB= ./libmysqlcppconn8.so.1.8.0.12

SO_LIB= 


.PHONY: all clean

all: ${TARGET}

${TARGET}: ${OBJS} ${LIB} 
	g++-7 -o $@ $^ ${LDFLAGS}${LIB} ${LIB64}  -lpthread  -lstdc++fs  -ldl -lssl -lcrypto
${CXXOBJS}:./%.o:./%.cc
	g++-7 -MMD -c -o $@ $< ${CFLAGS} 

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
	@echo cppfiels=$(cppfiles)
	@echo cfiels=$(cfiles)
	@echo DEPS=$(DEPS)
	@echo CXXOBJS=$(CXXOBJS)
	@echo COBJS=$(COBJS)

