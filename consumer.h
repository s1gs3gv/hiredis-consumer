#ifndef _CONSUMER_H
#define _CONSUMER_H

#define REDIS_HOST "localhost"
#define REDIS_PORT 6379
#define PUBLISH_CHANNEL "messages:published"
#define CONSUMER_GROUP "test_group"
#define STREAM_KEY "messages:processed"

#define MESSAGES_BUFFER_SIZE 1024

#define MAX_PROCESSED_MSGS 10000
// Message ids will be only UUID4 format for simplicity and avoiding memory fragmentation
#define MSG_ID_SIZE 36

#endif
