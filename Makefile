CC = g++
CFLAGS = -Wall

all: LoadBalancer

LoadBalancer: main.cpp
	$(CC) $(CFLAGS) main.cpp -o LoadBalancer