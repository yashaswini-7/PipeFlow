CC=gcc
CFLAGS=-Wall -Wextra -Wno-unused-parameter -g -pthread
pipeflow: src/pipeflow.c
	$(CC) $(CFLAGS) src/pipeflow.c -o pipeflow
clean:
	rm -f pipeflow
