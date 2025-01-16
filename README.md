# hiredis-consumer

### Prerequisites
To install dependencies on Ubuntu 
```
sudo apt-get install libjansson-dev
sudo apt-get install libhiredis-dev 
```

### Compiling the code
```
gcc consumer.c consumer.h -lhiredis -ljansson -I/usr/include/hiredis -I/usr/include/jansson -o consumer 
```

### Running the compiled code
The following will start two consumers with consumer ids 1 and 2 in a consumer group with maximum capacity of 2
```
./consumer -g 2 -c 1 
./consumer -g 2 -c 2
```
