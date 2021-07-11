#include <fcntl.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <stdio.h>
#include <netinet/in.h>
#include <resolv.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <pthread.h>
#include <iostream>
#include <vector>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <chrono>

#define SIZE 1024
#define PORT 9017

#define MAX_NUM_ARGUMENTS 10
#define WHITESPACE " \t\n" //command line tokenization
#define MAX_MESSAGE_SIZE 250

#define MAX_WORKERS 5

using namespace std;
using namespace google::protobuf::io;
using namespace chrono;

static atomic<int> num_workers{0};
static int uid = 10;


//void printInfo(const WorkersLog& workerslog);
void set_flag(int);
void* SocketHandler(void*);
void send_message(char*);
void str_trim_lf (char*, int);

/* Worker structure */
typedef struct{
    struct sockaddr_in address;
    int sockfd;
    int uid;
    char name[32];
} worker_t;

worker_t *workers[MAX_WORKERS];

pthread_mutex_t workers_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Add workers to queue */
void queue_add(worker_t *cl){
    pthread_mutex_lock(&workers_mutex);

    for(int i=0; i < MAX_WORKERS; ++i){
        if(!workers[i]){
            workers[i] = cl;
            break;
        }
    }

    pthread_mutex_unlock(&workers_mutex);
}

/* Remove workers to queue */
void queue_remove(int uid){
    pthread_mutex_lock(&workers_mutex);

    for(int i=0; i < MAX_WORKERS; ++i){
        if(workers[i]){
            if(workers[i]->uid == uid){
                workers[i] = NULL;
                break;
            }
        }
    }

    pthread_mutex_unlock(&workers_mutex);
}

int main(int argv, char** argc){

    struct sockaddr_in my_addr;
    vector<string> ip_addr;

    int hsock;
    int * p_int ;
    int err;

    socklen_t addr_size = 0;
    int* csock;
    sockaddr_in sadr;
    pthread_t thread_id=0;

    hsock = socket(AF_INET, SOCK_STREAM, 0);
    if(hsock == -1){
        printf("Error initializing socket %d\n", errno);
        goto FINISH;
    }

    p_int = (int*)malloc(sizeof(int));
    *p_int = 1;

    if( (setsockopt(hsock, SOL_SOCKET, SO_REUSEADDR, (char*)p_int, sizeof(int)) == -1 )||
        (setsockopt(hsock, SOL_SOCKET, SO_KEEPALIVE, (char*)p_int, sizeof(int)) == -1 ) ){
        printf("Error setting options %d\n", errno);
        free(p_int);
        goto FINISH;
    }
    free(p_int);

    my_addr.sin_family = AF_INET ;
    my_addr.sin_port = htons(PORT);

    memset(&(my_addr.sin_zero), 0, 8);
    my_addr.sin_addr.s_addr = INADDR_ANY ;

    if( bind( hsock, (sockaddr*)&my_addr, sizeof(my_addr)) == -1 ){
        fprintf(stderr,"Error binding to socket %d\n",errno);
        goto FINISH;
    }
    if(listen( hsock, 10) == -1 ){
        fprintf(stderr, "Error listening %d\n",errno);
        goto FINISH;
    }


    addr_size = sizeof(sockaddr_in);

    while(true){
        printf("waiting for a connection\n");
        csock = (int*)malloc(sizeof(int));
        if((*csock = accept( hsock, (sockaddr*)&sadr, &addr_size))!= -1){
            printf("---------------------\nReceived connection from %s\n",inet_ntoa(sadr.sin_addr));
            /*ip_addr.push_back(inet_ntoa(sadr.sin_addr));
            pthread_create(&thread_id,0,&SocketHandler, (void*)csock );
            pthread_detach(thread_id);*/

            /* Worker settings */
            worker_t *worker = (worker_t *)malloc(sizeof(worker_t));
            worker->address = sadr;
            worker->sockfd = *csock;
            worker->uid = uid++;

            /* Add worker to the queue and fork thread */
            queue_add(worker);


            pthread_create(&thread_id,0,&SocketHandler, (void*)worker);
            pthread_detach(thread_id);

            /* Reduce CPU usage */
            sleep(1);


        }
        else{
            fprintf(stderr, "Error accepting %d\n", errno);
        }
    }
    FINISH:
    ;
}

void* SocketHandler(void* lp) {
    char name[50];
    int leave_flag = 0;
    int bytecount = 0;
    char worker_message[SIZE] = {0};

    num_workers++;
    worker_t *worker = (worker_t *) lp;

    char buffer[4];
    char message[1024];
    char client_message[SIZE] = {0};

    memset(buffer, '\0', 4);

    if (recv(worker->sockfd, name, 32, 0) <= 0 || strlen(name) < 2 || strlen(name) >= 32 - 1) {
        cout << "error receiving name" << endl;
        leave_flag = 1;
    } else {
        strcpy(worker->name, name);
        cout << worker->name << " has joined\n" << endl;
        strcpy(message, worker->name);
    }


    while (1) {
        if (leave_flag) {
            cout << "leaving" << endl;
            break;
        }

        cout << "\n\n" << endl;
        cout << "Add a file : add <filepath> <fileName>" << endl;
        cout << "Remove a file: remove <file.txt> <filePath>" << endl;
        cout << "Search: query <file.txt>" << endl;
        cout << "Shutdown: shutdown" << endl;
        cout << "Exit: Ctrl + C" << endl;
        cout << "\n\n" << endl;

        while( !fgets (message, MAX_MESSAGE_SIZE, stdin) );

        cout << message << endl;
        char *token[5]; //parse message
        int token_count = 0;

        // Pointer to point to the token parsed by strsep
        char *arg_ptr;
        char *working_str = strdup(message);

        // move the working_str pointer to keep track of its original value so we can deallocate
        // the correct amount at the end
        char *working_root = working_str;

        // Tokenize the server_message with whitespace used as the delimiter
        while (((arg_ptr = strsep(&working_str, WHITESPACE)) != NULL) &&
               (token_count < MAX_NUM_ARGUMENTS)) {
            token[token_count] = strndup(arg_ptr, MAX_MESSAGE_SIZE);
            if (strlen(token[token_count]) == 0) {
                token[token_count] = NULL;
            }
            token_count++;
        }


        send_message(message);

        if (strcmp(message, "shutdown") == 0) {
            if ((bytecount = recv(worker->sockfd, client_message, SIZE, 0)) == -1) {
                cerr << "Error receiving data " << endl;
            }
            cout << client_message << endl;
            cout << "Shutting down leader ..." << endl;
            leave_flag = 1;
            goto FINISH;

        } else if ((strcmp(token[0], "make") == 0 && strcmp(token[1], "index") == 0) ||
                   ((strcmp(token[0], "add") == 0) && (token[1] != NULL) && (token[2] != NULL)) ||
                   ((strcmp(token[0], "query") == 0) && (token[1] != NULL)) ||
                   ((strcmp(token[0], "remove") == 0) && (token[1] != NULL) && (token[2] != NULL))) {
            if ((bytecount = recv(worker->sockfd, client_message, SIZE, 0)) == -1) {
                cerr << "Error receiving data " << endl;
            }
            cout << "in " << client_message << endl;
        }
        else
        {
            cout << "Invalid Command" <<endl;
            if ((bytecount = recv(worker->sockfd, client_message, SIZE, 0)) == -1) {
                cout << "Error receiving data " << endl;
            }
            else if(bytecount ==0){
                cout << "disconnected" <<endl;
            }
            cout << "in " << client_message << endl;
        }
        memset(client_message, 0, sizeof(client_message));
    }

    FINISH:
    close(worker->sockfd);
    queue_remove(worker->uid);
    free(worker);
    num_workers--;
    pthread_detach(pthread_self());
    return NULL;
}

void send_message(char* message){
    pthread_mutex_lock(&workers_mutex);
    cout << message <<endl;
    for (int i = 0; i < MAX_WORKERS; ++i) {
        if (workers[i]) {
            if (send(workers[i]->sockfd, message, strlen(message), 0) < 0) {
                cout << "Error sending message" << endl;
                break;
            }
        }
    }
    pthread_mutex_unlock(&workers_mutex);
}

void str_trim_lf (char* arr, int length) {
    int i;
    for (i = 0; i < length; i++) {
        if (arr[i] == '\n') {
            arr[i] = '\0';
            break;
        }
    }
}

