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
#include <iostream>
#include <deque>
#include <thread>
#include <atomic>
#include <mutex>
#include <tuple>
#include <vector>
#include <fstream>

#include "message.h"
#include "ouroboros.hpp"

#define SIZE 1024

#define NUM_INDEX_THREADS 4
#define BLOCK_SIZE 262144

#define MAX_NUM_ARGUMENTS 10
#define WHITESPACE " \t\n"
#define MAX_MESSAGE_SIZE 250
#define CAPACITY 1000 // Size of the Hash Table

#define QUEUE_SIZE_RATIO 4
#define DELIMITERS " \t\n/_.,;:"

char read_buffer[SIZE];
char client_message[SIZE];
int siz;


using namespace std;
using namespace ouroboros;

struct FilesQueue{
    deque<char*> queue;
    mutex mtx;
};

peer_t server;
char client_name[256];

typedef struct HTItems HTItems;
struct HTItems {
    char* filePath;
    char* fileName;
};


typedef struct LinkedList LinkedList;
struct LinkedList {
    HTItems* item;
    LinkedList* next;
};


typedef struct HashTable HashTable;
struct HashTable {
    HTItems** items;
    LinkedList** overflow_buckets;
    int size;
    int count;
};

unsigned long hashFunction(char* );
void printHT(HashTable*);
static LinkedList* allocate_list ();
static LinkedList* linkedlist_insert(LinkedList*, HTItems*);
static HTItems* linkedlist_removeFile(LinkedList*);
static void free_linkedlist(LinkedList* );
static LinkedList** create_overflow_buckets(HashTable* );
HTItems* create_item(char*, char*);
HashTable* create_table(int);
void free_item(HTItems*);
void free_table(HashTable*);
void handle_collision(HashTable*, unsigned long , HTItems*);
char* search(HashTable* , char* );
void printSearch( HashTable* , char* );
void printHT(HashTable*);

int index();
void addFile( HashTable*, char*, char*);
void removeFile(HashTable*, char*, char*);


void initialize(int,int, MemoryComponentManager*, FilesQueue*,
                vector<StdDirectTFIDFIndex*>*, vector<StdAppendFileIndex*>*,
                vector<thread>&, vector<thread>&);
void indexFile(char* , FilesQueue*);
void workRead(MemoryComponentManager*, FilesQueue*, StdAppendFileIndex*&,
              unsigned int, int);
void workIndex(MemoryComponentManager* , StdDirectTFIDFIndex*& ,unsigned int ,int );
void searchTerm(char* , int, vector<StdDirectTFIDFIndex*>*, vector<StdAppendFileIndex*>* );
void shutdown(int, MemoryComponentManager* , FilesQueue* ,
              vector<StdAppendFileIndex*>* ,
              vector<StdDirectTFIDFIndex*>* ,
              vector<thread>& ,
              vector<thread>&  );
void workInit(MemoryComponentManager*, unsigned int, int, int);

// create each queue in a specific NUMA node and add the queues to the manager
MemoryComponentManager* manager;
FilesQueue* files_queue;
vector<StdAppendFileIndex*>* fileIndexes;
vector<StdDirectTFIDFIndex*>* termsIndexes;
// list of threads that will read the contents of the input files
vector<thread> threads_read;

// list of threads that will index the contents of the input files
vector<thread> threads_index;


void shutdown_properly(int code);

void handle_signal_action(int sig_number)
{
    if (sig_number == SIGINT) {
        cout << "SIGINT caught" <<endl;
        shutdown_properly(EXIT_SUCCESS);
    }
    else if (sig_number == SIGPIPE) {
        cout << "SIGPIPE caught" <<endl;
        shutdown_properly(EXIT_SUCCESS);
    }
}

int setup_signals()
{
    struct sigaction sa;
    sa.sa_handler = handle_signal_action;
    if (sigaction(SIGINT, &sa, 0) != 0) {
        perror("sigaction()");
        return -1;
    }
    if (sigaction(SIGPIPE, &sa, 0) != 0) {
        perror("sigaction()");
        return -1;
    }

    return 0;
}

int get_client_name(int argc, char **argv, char *client_name)
{
    if (argc > 1)
        strcpy(client_name, argv[1]);
    else
        strcpy(client_name, "no name");

    return 0;
}

int connect_server(peer_t *server)
{
    // create socket
    server->socket = socket(AF_INET, SOCK_STREAM, 0);
    if (server->socket < 0) {
        perror("socket()");
        return -1;
    }

    // set up addres
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = inet_addr(SERVER_IPV4_ADDR);
    server_addr.sin_port = htons(PORT);

    server->addres = server_addr;

    if (connect(server->socket, (struct sockaddr *)&server_addr, sizeof(struct sockaddr)) != 0) {
        perror("connect()");
        return -1;
    }

    cout << "Connected to  " << SERVER_IPV4_ADDR << ": " << PORT << endl;


    return 0;
}


int build_fd_sets(peer_t *server, fd_set *read_fds, fd_set *write_fds, fd_set *except_fds)
{
    FD_ZERO(read_fds);
    FD_SET(server->socket, read_fds);

    FD_ZERO(write_fds);
    // there is smth to send, set up write_fd for server socket
    if (server->send_buffer.current > 0)
        FD_SET(server->socket, write_fds);

    FD_ZERO(except_fds);
    FD_SET(server->socket, except_fds);

    return 0;
}



/* You should be careful when using this function in multythread program.
 * Ensure that server is thread-safe. */
void shutdown_properly(int code)
{
    delete_peer(&server);
    cout << "shutdown client" <<endl;
    exit(code);
}

int handle_received_message(message_t *message)
{

    HashTable* ht = create_table(CAPACITY);
    memset(read_buffer, 0 , sizeof(read_buffer));
    memset(client_message, 0 , sizeof(client_message));
    cout << "Message received from server" << endl;
    print_message(message);
    sprintf(read_buffer,  "%s", message->data );
    strcpy(client_message, "hiii");




   /* // create each queue in a specific NUMA node and add the queues to the manager
    MemoryComponentManager* manager;
    manager = new MemoryComponentManager();
    FilesQueue* files_queue;

    vector<StdAppendFileIndex*>* fileIndexes;
    vector<StdDirectTFIDFIndex*>* termsIndexes;

    //allocating memory for files_queue
    files_queue = new FilesQueue{};

    //allocating memory for findex that stores files
    fileIndexes = new vector<StdAppendFileIndex*>();

    //allocating memory for index that stores terms
    termsIndexes = new vector<StdDirectTFIDFIndex*>();

    // list of threads that will read the contents of the input files
    vector<thread> threads_read;

    // list of threads that will index the contents of the input files
    vector<thread> threads_index;

    initialize(NUM_INDEX_THREADS, BLOCK_SIZE, manager, files_queue, termsIndexes,
               fileIndexes, threads_read, threads_index);
*/
    char *token[5]; //parse server_message
    int   token_count = 0;

    // Pointer to point to the token parsed by strsep
    char*arg_ptr;
    char *working_str  = strdup(read_buffer);

    // move the working_str pointer to keep track of its original value so we can deallocate
    // the correct amount at the end
    char *working_root = working_str;

    // Tokenize the server_message with whitespace used as the delimiter
    while ( ( (arg_ptr = strsep(&working_str, WHITESPACE ) ) != NULL) &&
            (token_count<MAX_NUM_ARGUMENTS))
    {
        token[token_count] = strndup( arg_ptr, MAX_MESSAGE_SIZE );
        if( strlen( token[token_count] ) == 0 )
        {
            token[token_count] = NULL;
        }
        token_count++;
    }
    char client_message[SIZE] = "NULL";

    if(strcmp(token[0], "shutdown")==0)
    {
        strcpy(client_message, "Shutting down client\n");
       shutdown(NUM_INDEX_THREADS, manager, files_queue,
                 fileIndexes,termsIndexes, threads_read, threads_index);
        shutdown_properly(2);

    }

    else if(strcmp(token[0], "make")==0 && strcmp(token[1], "index")==0)
    {
        cout << "size" << ht->size << endl;
        cout << "file" << ht->items[0]->fileName << endl;
        for (int i=0; i<ht->size; i++) {
            if (ht->items[i]) {
                cout << "here in index" << endl;
                indexFile( ht->items[i]->fileName, files_queue);
                if (ht->overflow_buckets[i]) {
                    LinkedList* head = ht->overflow_buckets[i];
                    while (head) {
                        indexFile(head->item->fileName, files_queue);
                        head = head->next;
                    }
                }
                printf("\n");
            }
        }
        printHT(ht);
    }

    else if((strcmp(token[0],"add")==0) && (token[1]!=NULL) && (token[2] !=NULL))
    {
        addFile(ht,token[1],token[2]);
        printHT(ht);
    }

    else if((strcmp(token[0],"query")==0) && (token[1]!=NULL))
    {
        searchTerm(token[1], NUM_INDEX_THREADS, termsIndexes, fileIndexes);

    }

    else if((strcmp(token[0],"remove")==0) && (token[1]!=NULL) && (token[2] !=NULL))
    {
        removeFile(ht,token[1],token[2]);
        printHT(ht);
    }
    else
    {
        strcpy(client_message, "Invalid");
        cout << client_message << endl;

        // Create new message and enqueue it.
        message_t new_message;
        prepare_message(client_name, client_message, &new_message);

        print_message(&new_message);


        if (peer_add_to_send(&server, &new_message) != 0) {
            cout << "Send buffer overflowed" << endl;
            return 0;
        }
    }

    return 0;
}

int main(int argc, char **argv)
{
   manager = new MemoryComponentManager();


   //allocating memory for files_queue
   files_queue = new FilesQueue{};

   //allocating memory for findex that stores files
   fileIndexes = new vector<StdAppendFileIndex*>();

   //allocating memory for index that stores terms
   termsIndexes = new vector<StdDirectTFIDFIndex*>();



   initialize(NUM_INDEX_THREADS, BLOCK_SIZE, manager, files_queue, termsIndexes,
              fileIndexes, threads_read, threads_index);


    if (setup_signals() != 0)
        exit(EXIT_FAILURE);

    char client_name[256];
    get_client_name(argc, argv, client_name);
    cout << client_name << " started" << endl;


    create_peer(&server);
    if (connect_server(&server) != 0)
        shutdown_properly(EXIT_FAILURE);

    /* Set nonblock for stdin. */
    int flag = fcntl(STDIN_FILENO, F_GETFL, 0);
    flag |= O_NONBLOCK;
    fcntl(STDIN_FILENO, F_SETFL, flag);

    fd_set read_fds;
    fd_set write_fds;
    fd_set except_fds;


    // server socket always will be greater then STDIN_FILENO
    int maxfd = server.socket;

    while (1) {
        // Select() updates fd_set's, so we need to build fd_set's before each select()call.
        build_fd_sets(&server, &read_fds, &write_fds, &except_fds);

        int activity = select(maxfd + 1, &read_fds, &write_fds, &except_fds, NULL);

        switch (activity) {
            case -1:
                perror("select()");
                shutdown_properly(EXIT_FAILURE);

            case 0:
                // you should never get here
                cout << "Select returned 0: Exit" << endl;
                shutdown_properly(EXIT_FAILURE);

            default:


                if (FD_ISSET(server.socket, &read_fds)) {
                    if (receive_from_peer(&server, &handle_received_message) != 0)
                        shutdown_properly(EXIT_FAILURE);
                }

                if (FD_ISSET(server.socket, &write_fds)) {
                    if (send_to_peer(&server) != 0)
                        shutdown_properly(EXIT_FAILURE);
                }

                if (FD_ISSET(server.socket, &except_fds)) {
                    cout << "Exception failure from leader" << endl;
                    shutdown_properly(EXIT_FAILURE);
                }
        }
    }

    return 0;
}

unsigned long hashFunction(char* str) {
    unsigned long i = 0;
    for (int j=0; str[j]; j++)
        i += str[j];
    return i % CAPACITY;
}

static LinkedList* allocate_list () {
    LinkedList* list = (LinkedList*) malloc (sizeof(LinkedList));
    return list;
}

static LinkedList* linkedlist_insert(LinkedList* list, HTItems* item) {
    if (!list) {
        LinkedList* head = allocate_list();
        head->item = item;
        head->next = NULL;
        list = head;
        return list;
    }

    else if (list->next == NULL) {
        LinkedList* node = allocate_list();
        node->item = item;
        node->next = NULL;
        list->next = node;
        return list;
    }

    LinkedList* temp = list;
    while (temp->next->next) {
        temp = temp->next;
    }
    LinkedList* node = allocate_list();
    node->item = item;
    node->next = NULL;
    temp->next = node;
    return list;
}

//removeFile head and return item of removeFiled elemet
static HTItems* linkedlist_removeFile(LinkedList* list) {
    if (!list)
        return NULL;
    if (!list->next)
        return NULL;
    LinkedList* node = list->next;
    LinkedList* temp = list;
    temp->next = NULL;
    list = node;
    HTItems* it = NULL;
    memcpy(temp->item, it, sizeof(HTItems));
    free(temp->item->filePath);
    free(temp->item->fileName);
    free(temp->item);
    free(temp);
    return it;
}

static void free_linkedlist(LinkedList* list) {
    LinkedList* temp = list;
    while (list) {
        temp = list;
        list = list->next;
        free(temp->item->fileName);
        free(temp->item->filePath);
        free(temp->item);
        free(temp);
    }
}

static LinkedList** create_overflow_buckets(HashTable* table) {
    // Create the overflow buckets; an array of linkedlists
    LinkedList** buckets = (LinkedList**) calloc (table->size, sizeof(LinkedList*));
    for (int i=0; i<table->size; i++)
        buckets[i] = NULL;
    return buckets;
}

static void free_overflow_buckets(HashTable* table) {
    // Free all the overflow bucket lists
    LinkedList** buckets = table->overflow_buckets;
    for (int i=0; i<table->size; i++)
        free_linkedlist(buckets[i]);
    free(buckets);
}


HTItems* create_item(char* filePath, char* fileName) {
    HTItems* item = (HTItems*) malloc (sizeof(HTItems));
    item->filePath = (char*) malloc (strlen(filePath) + 1);
    item->fileName = (char*) malloc (strlen(fileName) + 1);

    strcpy(item->filePath, filePath);
    strcpy(item->fileName, fileName);

    return item;
}

HashTable* create_table(int size) {
    HashTable* table = (HashTable*) malloc (sizeof(HashTable));
    table->size = size;
    table->count = 0;
    table->items = (HTItems**) calloc (table->size, sizeof(HTItems*));
    for (int i=0; i<table->size; i++)
        table->items[i] = NULL;
    table->overflow_buckets = create_overflow_buckets(table);

    return table;
}

void free_item(HTItems* item) {
    free(item->fileName);
    free(item->filePath);
    free(item);
}

void free_table(HashTable* table) {
    for (int i=0; i<table->size; i++) {
        HTItems* item = table->items[i];
        if (item != NULL)
            free_item(item);
    }

    free_overflow_buckets(table);
    free(table->items);
    free(table);
}

void handle_collision(HashTable* table, unsigned long index, HTItems* item) {
    LinkedList* head = table->overflow_buckets[index];
    if (head == NULL) {
        head = allocate_list();
        head->item = item;
        table->overflow_buckets[index] = head;
        return;
    }
    else {
        table->overflow_buckets[index] = linkedlist_insert(head, item);
        return;
    }
}

void addFile( HashTable* table, char* filePath, char* fileName) {
    HTItems* item = create_item(filePath, fileName);

    unsigned long indexHT = hashFunction(filePath);



    HTItems* current_item = table->items[indexHT];

    if (current_item == NULL) {
        if (table->count == table->size) {
            printf("Insert Error: Hash Table is full\n");

            free_item(item);
            goto FINISH;
        }
        // Insert directly
        table->items[indexHT] = item;
        table->count++;

        strcpy(client_message, "file added");

        // Create new message and enqueue it.
        message_t new_message;
        prepare_message(client_name, client_message, &new_message);

        print_message(&new_message);

        cout << "client_message" << client_message << endl;
        if (peer_add_to_send(&server, &new_message) != 0) {
            cout << "Send buffer overflowed" << endl;
        }
        cout << filePath << " successfully added\n" <<endl;
    }
    else {
        // Scenario 1: We only need to update filePath
        if (strcmp(current_item->filePath, filePath) == 0) {
            strcpy(table->items[indexHT]->fileName, fileName);

        }
        else {
            // Scenario 2: Collision
            handle_collision(table, indexHT, item);

        }
    }

    FINISH:
    ;

}

char* search(HashTable* table, char* filePath) {
    int index = hashFunction(filePath);
    HTItems* item = table->items[index];
    LinkedList* head = table->overflow_buckets[index];

    // Ensure that we move to items which are not NULL
    while (item != NULL) {
        if (strcmp(item->filePath, filePath) == 0)
            return item->fileName;
        if (head == NULL)
            return NULL;
        item = head->item;
        head = head->next;
    }
    return NULL;
}

void removeFile(HashTable* table, char* filePath, char* fileName) {
    // Deletes an item from the table
    int indexHT = hashFunction(filePath);
    HTItems* item = table->items[indexHT];
    LinkedList* head = table->overflow_buckets[indexHT];
    char* val = search(table, filePath);


    if (item == NULL) {//doesn't exist
        strcpy(client_message,"File doesn't exist");
        goto FINISH;
    }
    else {
        if (head == NULL && strcmp(item->filePath, filePath) == 0) {//no collsion
            table->items[indexHT] = NULL;
            free_item(item);
            table->count--;
            cout << filePath << " removed"<<endl;
            strcpy(client_message,"File removed");
            goto FINISH;
        }
        else if (head != NULL) { //collision, set head as new file
            if (strcmp(item->filePath, filePath) == 0) {
                free_item(item);
                LinkedList* node = head;
                head = head->next;
                node->next = NULL;
                table->items[indexHT] = create_item(node->item->filePath, node->item->fileName);
                free_linkedlist(node);
                table->overflow_buckets[indexHT] = head;
                cout << filePath << " removed"<<endl;
                strcpy(client_message,"File removed");
                goto FINISH;
            }
            LinkedList* curr = head;
            LinkedList* prev = NULL;
            while (curr) {
                if (strcmp(curr->item->filePath, filePath) == 0) {
                    if (prev == NULL) {
                        // First element of the chain. removeFile the chain
                        free_linkedlist(head);
                        table->overflow_buckets[indexHT] = NULL;
                        cout << filePath << " removed"<<endl;
                        strcpy(client_message,"File removed");
                        goto FINISH;
                    }
                    else {
                        // This is somewhere in the chain
                        prev->next = curr->next;
                        curr->next = NULL;
                        free_linkedlist(curr);
                        table->overflow_buckets[indexHT] = head;
                        cout << filePath << " removed"<<endl;
                        strcpy(client_message,"File removed");
                        goto FINISH;
                    }
                }
                curr = curr->next;
                prev = curr;
            }
        }
    }
    FINISH:
    ;
}

void printSearch(HashTable* table, char* filePath) {
    char* fileName;
    char temp[11];
    pid_t pid = getpid();
    sprintf(temp, "%d", pid);

    char pID[20] = "Query ID: ";
    strcat(pID, temp);


    if ((fileName = search(table, filePath)) == NULL) {
        printf("%s does not exist\n", filePath);
        strcpy(client_message, "Match for the query not found");
        goto FINISH;
    }
    else {
        printf("filePath:%s, fileName:%s\n", filePath, fileName);
        strcpy(client_message, "Match for the query found");

    }

    FINISH:
    ;
}

void printHT(HashTable* table) {
    printf("\n---------------------------------\n");
    for (int i=0; i<table->size; i++) {
        if (table->items[i]) {
            printf("Index:%d, filePath:%s, fileName:%s", i, table->items[i]->filePath, table->items[i]->fileName);
            if (table->overflow_buckets[i]) {
                printf(" => Overflow Bucket => ");
                LinkedList* head = table->overflow_buckets[i];
                while (head) {
                    printf("filePath:%s, fileName:%s ", head->item->filePath, head->item->fileName);
                    head = head->next;
                }
            }
            printf("\n");
        }
    }
    printf("----------------------------------\n");
}

void initialize(
        int num_indexers,
        int block_size,
        MemoryComponentManager* manager,
        FilesQueue *files_queue,
        vector<StdDirectTFIDFIndex*>* termsIndexes,
        vector<StdAppendFileIndex*>* fileIndexes,
        vector<thread>& threads_read,
        vector<thread>& threads_index){

    // the manager is used to store and provide access to NUMA sensitive components (i.e. the queues)

    FileDualQueueMemoryComponent* component;
    DualQueue<FileDataBlock*> *queue;
    FileDataBlock *finalBlock;

    // list of threads that initialize the NUMA sensitive components
    vector<thread> threads_init;

    int num_readers;
    int num_numa_nodes;
    int queue_size;
    NUMAConfig config;

    // find out how many NUMA nodes exist in the system
    num_numa_nodes = config.get_num_numa_nodes();
    num_numa_nodes = (num_indexers < num_numa_nodes) ? num_indexers : num_numa_nodes;
    num_readers = num_numa_nodes;


    queue_size = QUEUE_SIZE_RATIO * num_indexers / num_numa_nodes;

    for (int i = 0; i < num_numa_nodes; i++) {
        threads_init.push_back(thread(workInit, manager, i, queue_size, block_size));
    }

    for (int i = 0; i < num_numa_nodes; i++) {
        threads_init[i].join();
    }

    // check to see if the queues were allocated successfully
    for (int i = 0; i < num_numa_nodes; i++) {
        component = (FileDualQueueMemoryComponent*) manager->getMemoryComponent(MemoryComponentType::DUALQUEUE, i);
        if (!component->isAllocated()) {
            cout << "ERR: Could not allocate a buffer for the a queue!" << endl;

            // exit the benchmark on error
            return;
        }

        for (int i = 0; i < num_readers; i++) {
            fileIndexes->push_back(nullptr);
        }

        for (int i = 0; i < num_indexers; i++) {
            termsIndexes->push_back(nullptr);
        }

        for (int i = 0; i < num_readers; i++) {
            threads_read.push_back(thread(workRead,
                                          manager,
                                          files_queue,
                                          ref((*fileIndexes)[i]),
                                          i % num_numa_nodes,
                                          block_size));
        }

        for (int i = 0; i < num_indexers; i++) {
            threads_index.push_back(thread(workIndex,
                                           manager,
                                           ref((*termsIndexes)[i]),
                                           i % num_numa_nodes,
                                           block_size));
        }

        return;
    }
}

void indexFile(char* filePath, FilesQueue* files_queue)
{
    char* fileName;
    cout<< "-----here in workRead======-"<< endl;
    fileName =  new char[strlen(filePath)+1]{};
    strcpy(fileName, filePath);
    lock_guard<mutex> lock(files_queue->mtx);
    files_queue->queue.push_back(fileName);
}

void workRead(MemoryComponentManager* manager,
              FilesQueue* files_queue,
              StdAppendFileIndex*& fileIndex,
              unsigned int numa_id,
              int block_size)
{
    // self configure to run in numa_id NUMA node
    NUMAConfig config;
    config.set_task_numa_node(numa_id);

    {
        FileDualQueueMemoryComponent* component;
        DualQueue<FileDataBlock*> *queue;
        PFileReaderDriver *reader;
        FileDataBlock *dataBlock;
        char* fileName;
        bool noFile;
        long fileIdx;
        char *filePath;

        // get the queue component identified by numa_id from the manager
        component = (FileDualQueueMemoryComponent*) manager->getMemoryComponent(MemoryComponentType::DUALQUEUE,
                                                                                numa_id);
        // get the queue from the queue component
        queue = component->getDualQueue();

        //allocate memory for file index
        fileIndex = new StdAppendFileIndex();

        while(true){
            {
                noFile = false;
                lock_guard<mutex> lock(files_queue->mtx);
                if( files_queue->queue.size() > 0){
                    fileName = files_queue->queue.front();
                    files_queue->queue.pop_front();
                }
                else{
                    noFile = true;
                }
            }

            if(noFile){
                continue;
            }
            if(strcmp(fileName, "shutdown")==0){
                delete fileName;
                break;
            }


            // create a new reader driver for the current file
            reader = new PFileReaderDriver(fileName, block_size);

            // try to open the file in the reader driver; if it errors out print message and skip the file
            try {
                reader->open();
            } catch (exception &e) {
                cout << "ERR: could not open file " << fileName << endl;
                strcpy(client_message, "Could not open file : ");
                strcat(client_message, fileName);
                // Create new message and enqueue it.
                message_t new_message;
                prepare_message(client_name, client_message, &new_message);

                print_message(&new_message);

                cout << "client_message" << client_message << endl;
                if (peer_add_to_send(&server, &new_message) != 0) {
                    cout << "Send buffer overflowed" << endl;
                }
                delete reader;
                continue;
            }

            fileIdx = fileIndex->insert(fileName);
            filePath = (char*) fileIndex->reverseLookup(dataBlock->fileIdx);


            while (true) {
                // pop empty data block from the queue
                dataBlock = queue->pop_empty();

                // read a block of data from the file into data block buffer
                reader->readNextBlock(dataBlock);

                dataBlock->fileIdx = fileIdx;
                dataBlock->filepath = filePath;

                // push full data block to queue (in this case it pushed to the empty queue since there is no consumer)
                queue->push_full(dataBlock);

                // if the reader driver reached the end of the file break from the while loop and read next file
                if (dataBlock->length == 0) {
                    break;
                }
            }

            // close the reader and free memory
            reader->close();
            delete reader;
            delete fileName;

        }
    }
}

void workIndex(MemoryComponentManager* manager,
               StdDirectTFIDFIndex*& termsIndex,
               unsigned int numa_id,
               int block_size)
{
    NUMAConfig config;
    config.set_task_numa_node(numa_id);
    int indexed=0;
    {
        FileDualQueueMemoryComponent* component;
        DualQueue<FileDataBlock*> *queue;
        FileDataBlock *dataBlock;
        BranchLessTokenizer *tokenizer;
        CTokBLock *tokBlock;
        char *buffer;
        char **tokens;
        char delims[32] = DELIMITERS;
        long num_tokens(0);
        int length;

        // allocate the buffers, the list of tokens for the tokenizer data block and create the
        buffer = new char[block_size + 1];
        tokens = new char*[block_size / 2 + 1];
        tokBlock = new CTokBLock();
        tokBlock->buffer = buffer;
        tokBlock->tokens = tokens;
        tokenizer = new BranchLessTokenizer(delims);

        // get the queue component identified by numa_id from the manager
        component = (FileDualQueueMemoryComponent*) manager->getMemoryComponent(MemoryComponentType::DUALQUEUE,
                                                                                numa_id);
        // get the queue from the queue component
        queue = component->getDualQueue();

        //allocate memory for indexers to return
        termsIndex = new StdDirectTFIDFIndex();

        // load balancing is achieved through the queue
        while (true) {
            // pop full data block from the queue
            dataBlock = queue->pop_full();

            // if the data in the block has a length greater than 0 then tokenzie, otherwise exit the while loop
            length = dataBlock->length;
            if (length > 0) {
                //this is where the blocks are being tokenized
                tokenizer->getTokens(dataBlock, tokBlock);
                //this is where the tokens are being indexed
                for (long i = 0; i < tokBlock->numTokens; i++) {
                    termsIndex->insert(tokBlock->tokens[i], dataBlock->fileIdx);
                }
            }
            if(length == 0){
                indexed=1;
                cout << "File: " << dataBlock->filepath << " finish indexing " << endl;
                strcpy(client_message, "Indexing successful in: ");
                //strcat(client_message, dataBlock->filepath);
                // Create new message and enqueue it.
                message_t new_message;
                prepare_message(client_name, client_message, &new_message);

                print_message(&new_message);

                cout << "client_message" << client_message << endl;
                if (peer_add_to_send(&server, &new_message) != 0) {
                    cout << "Send buffer overflowed" << endl;
                }

            }

            queue->push_empty(dataBlock);

            if (length == -1) {

                strcpy(client_message, "Indexing Unsuccessful : ");
                //strcat(client_message, dataBlock->filepath);
                // Create new message and enqueue it.
                message_t new_message;
                prepare_message(client_name, client_message, &new_message);

                print_message(&new_message);

                cout << "client_message" << client_message << endl;
                if (peer_add_to_send(&server, &new_message) != 0) {
                    cout << "Send buffer overflowed" << endl;
                }
                break;
            }
        }
        delete tokenizer;
        delete tokBlock;
        delete[] tokens;
        delete[] buffer;

    }
}


void searchTerm(char* search_term,
                int num_indexers,
                vector<StdDirectTFIDFIndex*>* termsIndexes,
                vector<StdAppendFileIndex*>* fileIndexes){

    //term unique ID
    long searchTermIdx;

    //file unique ID
    long fileIdx;
    const char* fileName = nullptr;

    int num_readers;
    int num_numa_nodes;
    NUMAConfig config;


    // find out how many NUMA nodes exist in the system
    num_numa_nodes = config.get_num_numa_nodes();
    num_numa_nodes = (num_indexers < num_numa_nodes) ? num_indexers : num_numa_nodes;
    num_readers = num_numa_nodes;

    //term string value, file unique ID, frequency of the term
    tuple< const char*, long, long> lookupResult;

    for (int i = 0; i < num_indexers; i++) {
        searchTermIdx = (*termsIndexes)[i]->lookup(search_term);
        if (searchTermIdx != -1) {
            cout << "found " << searchTermIdx << endl;
            lookupResult = (*termsIndexes)[i]->reverseLookup(searchTermIdx);
            //get unique ID from lookup result
            fileIdx = get<1>(lookupResult);
            fileName = (*fileIndexes)[i%num_readers]->reverseLookup(fileIdx);
            break;
        }
    }

    // query1->set_result_found("Match for the query found");
    if(fileName == nullptr){
        strcpy(client_message, "No match found" );
    }
    else{
        /*ofstream file;
        file.open("/exports/stor/data/data.txt",ios_base::app);
        file << filename<< endl;*/
        strcpy(client_message, "Results added in the shared file" );
    }
    message_t new_message;
    prepare_message(client_name, client_message, &new_message);

    print_message(&new_message);

    cout << "client_message: " << client_message << endl;
    if (peer_add_to_send(&server, &new_message) != 0) {
        cout << "Send buffer overflowed" << endl;
    }

}

void shutdown( int num_indexers,
               MemoryComponentManager* manager,
               FilesQueue* files_queue,
               vector<StdAppendFileIndex*>* fileIndexes,
               vector<StdDirectTFIDFIndex*>* termsIndexes,
               vector<thread>& threads_read,
               vector<thread>& threads_index )
{
    int num_readers;
    int num_numa_nodes;
    NUMAConfig config;

    FileDualQueueMemoryComponent* component;
    DualQueue<FileDataBlock*> *queue;
    FileDataBlock *finalBlock;

    // find out how many NUMA nodes exist in the system
    num_numa_nodes = config.get_num_numa_nodes();
    num_numa_nodes = (num_indexers < num_numa_nodes) ? num_indexers : num_numa_nodes;
    num_readers = num_numa_nodes;

    for(int i=0; i<num_readers; i++){
        char* buffer = new char[strlen("shutdown")+1]{};
        strcpy(buffer, "shutdown");
        lock_guard<mutex> lock(files_queue->mtx);
        files_queue->queue.push_back(buffer);
    }

    for (int i = 0; i < num_readers; i++) {
        threads_read[i].join();
    }

    for (int i = 0; i < num_numa_nodes; i++) {
        for (int j = 0; j < num_indexers / num_numa_nodes + 1; j++) {
            component = (FileDualQueueMemoryComponent*) manager->getMemoryComponent(MemoryComponentType::DUALQUEUE, i);
            queue = component->getDualQueue();
            finalBlock = queue->pop_empty();
            finalBlock->length = -1;
            queue->push_full(finalBlock);
        }
    }

    for (int i = 0; i < num_indexers; i++) {
        threads_index[i].join();
    }
}

void workInit(MemoryComponentManager* manager, unsigned int numa_id, int queue_size, int block_size)
{
    // self configure to run in numa_id NUMA node
    NUMAConfig config;
    config.set_task_numa_node(numa_id);

    {
        FileDualQueueMemoryComponent* component;

        // create a new queue component; the component is responsible with intializing the queue and the queue elements
        component = new FileDualQueueMemoryComponent(queue_size, block_size);

        // add the queue component to the manager identified by the current numa_id
        manager->addMemoryComponent(MemoryComponentType::DUALQUEUE, numa_id, component);
    }
}
