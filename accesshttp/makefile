TARGET  := accesshttp.exe
INCS = -I ../network -I ../utils -I ../../libjwt/include
#LIBS = -L ../utils -lutils -L ../network -lnetwork -Wl,-Bstatic -lev -ljansson -lmpdec -lrdkafka -ljwt -lz -lssl -lcrypto -lhiredis -Wl,-Bdynamic -lm -lpthread -ldl
LIBS = ../../libjwt/libjwt/.libs/libjwt.a -L ../utils -lutils -L ../network -lnetwork -Wl,-Bstatic -lev -ljansson -lmpdec -lrdkafka -lz -lssl -lcrypto -lhiredis -Wl,-Bdynamic -lm -lpthread -ldl
include ../makefile.inc
