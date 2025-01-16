#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <hiredis.h>
#include <getopt.h>
#include <time.h>
#include <signal.h>
#include <jansson.h>

#include "consumer.h"

redisContext *global_redis_context = NULL;

void help(const char *program) {
    printf("Usage: %s [options]\n", program);
    printf("Consumes messages from redis with specified number of consumers," 
            "where multiple consumers can independently process messages from the same stream.\n"
            "Reports periodically number of processed messages.\n");
    printf("Options:\n");
    printf("  -c, --consumer-id    Consumer ID (integer)\n");
    printf("  -g, --group-size     Consumer group size (integer)\n");
    printf("  -h, --host           Redis host (default: %s)\n", REDIS_HOST);
    printf("  -p, --port           Redis port (default: %d)\n", REDIS_PORT);
    printf("  -v, --verbose        Enable verbose output\n");
    printf("  -?, --help           Show this help message\n");
}

typedef struct {
    char message_id[37]; // UUID is 36 characters + 1 for null terminator; this will be used for the json parser
} Message;

typedef struct {
    char **processed_message_ids;
    int processed_message_count;
} consumerState;

consumerState *global_consumer_state = NULL;

void freeConsumerState(consumerState *state) {
    if (state != NULL) {
        for (int i = 0; i < state->processed_message_count; i++) {
            free(state->processed_message_ids[i]);
        }
        free(state->processed_message_ids);
        free(state);
    }
}

consumerState* createConsumerState() {
    consumerState *state = (consumerState*)malloc(sizeof(consumerState));
    state->processed_message_ids = (char**)malloc(sizeof(char*) * MAX_PROCESSED_MSGS);
    state->processed_message_count = 0;
    
    return state;
}

int isMessageProcessed(const char *message_id) {
    for (int i = 0; i < global_consumer_state->processed_message_count; i++) {
        if (strcmp(global_consumer_state->processed_message_ids[i], message_id) == 0) {
            return 1;  // Message already processed
        }
    }
    return 0;  // Message not processed
}

void addProcessedMessage(const char *message_id) {
    if (global_consumer_state->processed_message_count < MAX_PROCESSED_MSGS) {
        global_consumer_state->processed_message_ids[global_consumer_state->processed_message_count] = (char *)malloc(sizeof(char)* (strlen(message_id) + 1));
        strncpy(global_consumer_state->processed_message_ids[global_consumer_state->processed_message_count], message_id, strlen(message_id));
        global_consumer_state->processed_message_count++;
    } else {
        // Handle memory overflow by skipping new messages
        fprintf(stderr, "Warning: Processed message limit reached for consumer. Skipping adding new IDs.\n");
    }
}

int parseMessage(const char *json_string, Message *message) {
    json_t *root;
    json_error_t error;

    root = json_loads(json_string, 0, &error);

    if (!root) {
        fprintf(stderr, "Error parsing JSON on line %d: %s\n", error.line, error.text);
        return -1;
    }

    // Get the "message_id" field
    json_t *message_id = json_object_get(root, "message_id");
    if (!json_is_string(message_id)) {
        fprintf(stderr, "Error: 'message_id' is missing or not a string\n");
        json_decref(root);
        return -1;
    }

    strncpy(message->message_id, json_string_value(message_id), sizeof(message->message_id) - 1);
    message->message_id[sizeof(message->message_id) - 1] = '\0';

    json_decref(root); // Free the JSON object
    return 0;
}

void processMessage(redisContext *c, const char *message, int consumer_id) {
    printf("Received message: %s\n", message);

    json_error_t error;
    json_t *json_msg = json_loads(message, 0, &error);

    Message parsed_message;

    // Parse the JSON and populate the struct
    if (parseMessage(message, &parsed_message) == 0) {
        printf("Parsed message_id: %s\n", parsed_message.message_id);
    } else {
        printf("Failed to parse the JSON\n");
    }

    // Check if the message has already been processed
    if (isMessageProcessed(parsed_message.message_id)) {
        printf("Consumer %d skipping already processed message: %s\n", consumer_id, parsed_message.message_id);
        return;
    }

    // Simulate processing by adding consumer ID
    json_object_set_new(json_msg, "message_id", json_string(parsed_message.message_id));
    json_object_set_new(json_msg, "consumer_id", json_integer(consumer_id));

    // Convert the JSON object back to a string
    char *modified_message = json_dumps(json_msg, JSON_COMPACT);
    if (!modified_message) {
        fprintf(stderr, "Error serializing JSON object for message: %s\n", parsed_message.message_id);
        json_decref(json_msg); // Free JSON object
        return;
    }

    // Print the processed message
    printf("Processed message: %s\n", modified_message);

    // Store the processed message in Redis
    redisReply *reply = redisCommand(c, "XADD %s * message_id %s consumer_id %d", STREAM_KEY, parsed_message.message_id, consumer_id);
    if (!reply || c->err) {
        fprintf(stderr, "Error storing processed message in Redis: %s\n", c->errstr);
        if (reply) freeReplyObject(reply);
        free(modified_message);
        return;
    }

    freeReplyObject(reply);

    // Track the message ID as processed locally
    addProcessedMessage(parsed_message.message_id);

    // Free dynamically allocated resources
    free(modified_message);
}

void shutdown(int signum) {
    if (global_redis_context != NULL) {
        printf("\nCleaning up redis context...\n");
        redisFree(global_redis_context);
    }
    if (global_consumer_state != NULL) {
        printf("\nCleaning up consumer state...\n");
        freeConsumerState(global_consumer_state);
    }
    exit(0);
}

int main(int argc, char **argv) {
    int consumer_group_size = -1;
    int consumer_id = -1;
    const char *redis_host = REDIS_HOST;
    int redis_port = REDIS_PORT;
    int verbose = 0;
    
    // Command-line arguments options for parsing
    static struct option long_options[] = {
        {"consumer-id", required_argument, NULL, 'c'},
        {"group-size", required_argument, NULL, 'g'},
        {"host", required_argument, NULL, 'h'},
        {"port", required_argument, NULL, 'p'},
        {"verbose", no_argument, NULL, 'v'},
        {"help", no_argument, NULL, '?'},
        {NULL, 0, NULL, 0}
    };

    int option_index = 0;
    int opt;
    while ((opt = getopt_long(argc, argv, "c:g:h:p:v?", long_options, &option_index)) != -1) {
        switch (opt) {
            case 'g':
                consumer_group_size = atoi(optarg);
                if (consumer_group_size <= 0) {
                    fprintf(stderr, "Invalid consumer group size\n");
                    help(argv[0]);
                    exit(EXIT_FAILURE);
                }
                break;
            case 'c':
                consumer_id = atoi(optarg);
                if (consumer_id <= 0) {
                    fprintf(stderr, "Invalid consumer id\n");
                    help(argv[0]);
                    exit(EXIT_FAILURE);
                } else if (consumer_id > consumer_group_size) {
                    fprintf(stderr, "Too many consumers. Maximum number of consumers is %d\n", consumer_group_size);
                    exit(EXIT_FAILURE);
                }
                break;
            case 'h':
                redis_host = optarg;
                break;
            case 'p':
                redis_port = atoi(optarg);
                break;
            case 'v':
                verbose = 1;
                break;
            case '?':
                help(argv[0]);
                exit(EXIT_SUCCESS);
                break;
            default:
                help(argv[0]);
                exit(EXIT_FAILURE);
        }
    }

    if (consumer_group_size == -1 || consumer_id == -1) {
        fprintf(stderr, "Consumer group size and consumer id are mandatory\n");
        help(argv[0]);
        exit(EXIT_FAILURE);
    }

    // Setup signal handlers for graceful shutdown
    signal(SIGINT, shutdown);  // Catch user interruption signal - Ctrl+C
    signal(SIGTERM, shutdown); // Catch process termination signal

    // Connect to Redis server
    redisContext *c = redisConnect(redis_host, redis_port);
    if (c == NULL) {
        fprintf(stderr, "Error allocating redis context\n");
        exit(EXIT_FAILURE);
    } else if (c && c->err) {
        fprintf(stderr, "Error connecting to redis server: %s\n", c->errstr);
        exit(EXIT_FAILURE);
    }
    global_redis_context = c; // Store the global context for cleanup

    // Create a consumer group (if it doesn't aleady exist)
    redisReply *reply = redisCommand(c, "XGROUP CREATE %s %s 0 MKSTREAM", STREAM_KEY, CONSUMER_GROUP);
    if (reply == NULL || c->err) {
        fprintf(stderr, "Error creating consumer group: %s\n", c->errstr);
        redisFree(c);
        exit(EXIT_FAILURE);
    }
    freeReplyObject(reply);

    // Create redisReader
    redisReader *reader = redisReaderCreate();
    if (!reader) {
        fprintf(stderr, "Error creating redisReader\n");
        redisFree(c);
        exit(EXIT_FAILURE);
    }

    // Subscribe to the publish channel
    reply = redisCommand(c, "SUBSCRIBE %s", PUBLISH_CHANNEL);
    if (reply == NULL || c->err) {
        fprintf(stderr, "Error subscribing to channel: %s\n", c->errstr);
        redisFree(c);
        redisReaderFree(reader);
        exit(EXIT_FAILURE);
    }

    // Check if channel subscription is successful
    if (reply->type == REDIS_REPLY_ARRAY && reply->elements >= 3) {
        const char *channel = reply->element[1]->str;
        printf("Successfully subscribed to channel: %s\n", channel);
    }
    freeReplyObject(reply);

    // Create consumer state
    global_consumer_state = createConsumerState();

    // Monitor processed messages
    time_t start_time = time(NULL);
    int processed_messages = 0;

    while (1) {
        redisReply *reply = NULL;
        size_t bytes_read;
        char messages[MESSAGES_BUFFER_SIZE];
        int n = read(c->fd, messages, sizeof(messages));

        if (n > 0) {
            redisReaderFeed(reader, messages, n);
            int res = redisReaderGetReply(reader, (void**)&reply);

            if (res == REDIS_OK && reply) {
                if (reply->type == REDIS_REPLY_ARRAY && reply->elements >= 3) {
                    const char *message = reply->element[2]->str;

                    processMessage(c, message, consumer_id);
                    processed_messages++;

                    time_t current_time = time(NULL);
                    if (difftime(current_time, start_time) >= 3) {
                        printf("Processed messages per second: %d\n", processed_messages / 3);
                        processed_messages = 0;
                        start_time = current_time;
                    }
                }

                freeReplyObject(reply);
            } else if (res == REDIS_ERR) {
                fprintf(stderr, "Error reading reply: %s\n", c->errstr);
                break;
            }
        } else if (n == 0) {
            fprintf(stderr, "Connection closed by server\n");
            break;
        } else {
            fprintf(stderr, "Error reading from socket");
            break;
        }

        usleep(1000); 
    }

    shutdown(0);
    return 0;
}
