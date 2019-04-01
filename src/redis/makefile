TARGET = ../../lib/lib.a
OBJPATH = ./
CFLAGS = -Wall -w  -g -ggdb -O0 -Wno-unused -Wno-sign-compare -Wno-deprecated-declarations -Wno-deprecated -Wl,--no-as-needed -std=c++17  


LFLAGS = -lpthread -lstdc++fs
LLIB = 

OBJS = $(patsubst %.cc,$(OBJPATH)/%.o, $(notdir $(wildcard *.cc)))

$(OBJPATH)/%.o : %.cc
	g++ -c $(CFLAGS) $< -o $@

all : $(TARGET)

$(TARGET) : $(OBJS)
	$(AR) -rcs $(OBJPATH)/$(TARGET) $(OBJS) 

install:
	@mkdir /usr/local/include/xredis
	@cp *.h /usr/local/include/xredis
	
clean:
	@rm -f $(OBJPATH)/*.o
	@rm -f $(TARGET)
	@rm -rf /usr/local/include/xredis
	
	
	

