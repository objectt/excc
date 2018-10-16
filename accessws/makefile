TARGET  := accessws.exe
INCS = -I ../network -I ../utils
LIBS = -L ../../libs -Wl,-Bstatic -L ../utils -lutils -L ../network -lnetwork -lev -ljansson -lmpdec -lrdkafka -lz -lssl -lhiredis -lcurl -lcrypto -Wl,-Bdynamic -lm -lpthread -ldl
include ../makefile.inc
