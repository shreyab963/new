#include <chrono>
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
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include "message.pb.h"
#include <iostream> 
#include <google/protobuf/message.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <deque>
#include <thread>
#include <atomic>
#include <mutex>
#include <tuple>
#include <vector>


#include "ouroboros.hpp"

#define SIZE 1000
#define PORT 9017


#define NUM_INDEX_THREADS 4
#define BLOCK_SIZE 262144

#define NAME_LEN 256


#define MAX_NUM_ARGUMENTS 10
#define WHITESPACE " \t\n" 
#define MAX_MESSAGE_SIZE 250 
#define CAPACITY 1000 // Size of the Hash Table

#define QUEUE_SIZE_RATIO 4
#define DELIMITERS " \t\n/_.,;:"

char* pkt;
int siz;

using namespace google::protobuf::io;
using namespace std;
using namespace std::chrono;
using namespace ouroboros;

struct FilesQueue{
    deque<char*> queue;
    mutex mtx;
};

typedef struct HTItems HTItems;
struct HTItems {
    char* fileName;
    char* filePath;
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
void printSearch(Worker*, HashTable* , char* );
void printHT(HashTable*);
 
int index(Worker*);
int addFile(Worker*, HashTable*, char*, char*);
int removeFile(Worker*, HashTable*, char*, char*);


void initialize(int,int, MemoryComponentManager*, FilesQueue*,
                                  vector<StdDirectTFIDFIndex*>*, vector<StdAppendFileIndex*>*,
                                  vector<thread>&, vector<thread>&);
void indexFile(char* , FilesQueue*);
void workRead(MemoryComponentManager*, FilesQueue*, StdAppendFileIndex*&,
              unsigned int, int); 
void workIndex(MemoryComponentManager* , StdDirectTFIDFIndex*& ,unsigned int ,int );
void searchTerm(Worker*,char* , int, vector<StdDirectTFIDFIndex*>*, vector<StdAppendFileIndex*>* );
void shutdown(int, MemoryComponentManager* , FilesQueue* , 
                vector<StdAppendFileIndex*>* ,
                vector<StdDirectTFIDFIndex*>* ,
                vector<thread>& ,
     vector<thread>&  );
void workInit(MemoryComponentManager*, unsigned int, int, int);


int main(int argv, char** argc){

        WorkersLog workers_log;
        HashTable* ht = create_table(CAPACITY);
        Worker worker;

        char host_name[]="172.20.3.153";
        int size;
        struct sockaddr_in my_addr;

        char buffer[1024];
        char server_message[1024]={0};
        int bytecount;
        int buffer_len=0;

        int hsock;
        int * p_int;
        int err;

        // create each queue in a specific NUMA node and add the queues to the manager
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


        hsock = socket(AF_INET, SOCK_STREAM, 0);
        if(hsock == -1){
                printf("Error initializing socket %d\n",errno);
                goto FINISH;
        }

        p_int = (int*)malloc(sizeof(int));
        *p_int = 1;

        if( (setsockopt(hsock, SOL_SOCKET, SO_REUSEADDR, (char*)p_int, sizeof(int)) == -1 )||
                (setsockopt(hsock, SOL_SOCKET, SO_KEEPALIVE, (char*)p_int, sizeof(int)) == -1 ) ){
                printf("Error setting options %d\n",errno);
                free(p_int);
                goto FINISH;
        }
        free(p_int);

        my_addr.sin_family = AF_INET ;
        my_addr.sin_port = htons(PORT);

        
        memset(&(my_addr.sin_zero), 0, 8);
        my_addr.sin_addr.s_addr = inet_addr(host_name);
        if( connect( hsock, (struct sockaddr*)&my_addr, sizeof(my_addr)) == -1 ){
                if((err = errno) != EINPROGRESS){
                        fprintf(stderr, "Error connecting socket %d\n", errno);
                        goto FINISH;
                }
        }

       char hostName[NAME_LEN];
       int hostN;

        hostN = gethostname(hostName, NAME_LEN);
        if( (bytecount=send(hsock, hostName ,NAME_LEN,0))== -1 )
         {
        cerr << "Error sending name\n "  << endl;
        goto FINISH;
         }


    while( (bytecount = recv(hsock , server_message , SIZE, 0)) > 0 )
        {
            cout << server_message << endl;

            char *token[5]; //parse server_message
            int   token_count = 0;                                 
                                                           
            // Pointer to point to the token parsed by strsep
            char *arg_ptr;                                                                                       
            char *working_str  = strdup(server_message);                

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
                if( (bytecount=send(hsock, client_message ,SIZE,0))== -1 ) 
                {
                        cerr << "Error sending shutdown message\n "  << endl;
                        goto FINISH;
                }
                cout << client_message << endl;
                return 0;
            }

            else if(strcmp(token[0], "make")==0 && strcmp(token[1], "index")==0)
            {
		//auto start = high_resolution_clock::now();
                for (int i=0; i<ht->size; i++) {
                if (ht->items[i]) {
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
		//auto stop = high_resolution_clock::now();
		//auto duration = duration_cast<microseconds>(stop - start);
		cout << duration.count() << endl;
                siz= index(workers_log.add_workers());             
                if( (bytecount=send(hsock,(void*)pkt,siz,0))== -1 ) 

                {
                        cerr << "Error sending data\n " << endl;   
                }
                printHT(ht);
            }

            else if((strcmp(token[0],"add")==0) && (token[1]!=NULL) && (token[2] !=NULL))
            {
               siz= addFile(workers_log.add_workers(),ht,token[1],token[2]);            
                if( (bytecount=send(hsock,(void*)pkt,siz,0))== -1 ) 
                {
                        cerr << "Error sending data\n " << endl;   
                }
                printHT(ht);
            }

            else if((strcmp(token[0],"query")==0) && (token[1]!=NULL))
            {
                searchTerm(workers_log.add_workers(),token[1], NUM_INDEX_THREADS, termsIndexes, fileIndexes);

                // printSearch(workers_log.add_workers(),ht,token[1]); 
                if( (bytecount=send(hsock,(void*)pkt,siz,0))== -1 ) 
                {
                        cerr << "Error sending data\n " << endl;   
                }
            }

            else if((strcmp(token[0],"remove")==0) && (token[1]!=NULL) && (token[2] !=NULL))
            {
               siz= removeFile(workers_log.add_workers(),ht,token[1],token[2]);             
                if( (bytecount=send(hsock,(void*)pkt,siz,0))== -1 ) 
                {
                        cerr << "Error sending data\n " << endl;   
                }
                printHT(ht);
            }
            else
            {
                strcpy(client_message, "Invalid\n");
                if( (bytecount=send(hsock, client_message ,SIZE,0))== -1 ) 
                {
                        cerr << "Error sending shutdown message\n "  << endl;
                        goto FINISH;
                }
                cout << client_message << endl;
            }
            memset(server_message, 0, sizeof(server_message));
        }  
        delete pkt;
FINISH:
        close(hsock);
}

int index(Worker* worker)
{
    Worker::Index* index = worker->add_indexes();
    siz = worker->ByteSizeLong()+4;
    pkt = new char [siz];
    google::protobuf::io::ArrayOutputStream aos(pkt,siz);
    CodedOutputStream *coded_output = new CodedOutputStream(&aos);
    coded_output->WriteVarint32(worker->ByteSizeLong());
    worker->SerializeToCodedStream(coded_output);
return siz;
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
    free(temp->item->fileName);
    free(temp->item->filePath);
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
 
 
HTItems* create_item(char* fileName, char* filePath) {
    HTItems* item = (HTItems*) malloc (sizeof(HTItems));
    item->fileName = (char*) malloc (strlen(fileName) + 1);
    item->filePath = (char*) malloc (strlen(filePath) + 1);
     
    strcpy(item->fileName, fileName);
    strcpy(item->filePath, filePath);
 
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
 
int addFile(Worker* worker, HashTable* table, char* fileName, char* filePath) {
    HTItems* item = create_item(fileName, filePath);
   
    unsigned long indexHT = hashFunction(fileName);
    Worker::Index* index = worker->add_indexes();
    index->set_file_name(fileName);
    index->set_file_path(filePath);
 
    HTItems* current_item = table->items[indexHT];
     
    if (current_item == NULL) {
        if (table->count == table->size) {
            printf("Insert Error: Hash Table is full\n");
            index->set_message("No storage left in the disk");
            free_item(item);
            goto FINISH;
        } 
        // Insert directly
        table->items[indexHT] = item; 
        table->count++;
        index->set_message("File Successfully Added");
        cout << fileName << " successfully added\n" <<endl;
    }
    else {
            // Scenario 1: We only need to update filePath
            if (strcmp(current_item->fileName, fileName) == 0) {
                strcpy(table->items[indexHT]->filePath, filePath);
                index->set_message("File Successfully Added");
                goto FINISH;
            }
        else {
            // Scenario 2: Collision
            handle_collision(table, indexHT, item);
            goto FINISH;
        }
    }

FINISH:
    siz = worker->ByteSizeLong()+4;
    pkt = new char [siz];
    google::protobuf::io::ArrayOutputStream aos(pkt,siz);
    CodedOutputStream *coded_output = new CodedOutputStream(&aos);
    coded_output->WriteVarint32(worker->ByteSizeLong());
    worker->SerializeToCodedStream(coded_output); 
    return siz;
}
 
char* search(HashTable* table, char* fileName) {
    int index = hashFunction(fileName);
    HTItems* item = table->items[index];
    LinkedList* head = table->overflow_buckets[index];
 
    // Ensure that we move to items which are not NULL
    while (item != NULL) {
        if (strcmp(item->fileName, fileName) == 0)
            return item->filePath;
        if (head == NULL)
            return NULL;
        item = head->item;
        head = head->next;
    }
    return NULL;
}

int removeFile(Worker* worker,HashTable* table, char* fileName, char* filePath) {
    // Deletes an item from the table
    int indexHT = hashFunction(fileName);
    HTItems* item = table->items[indexHT];
    LinkedList* head = table->overflow_buckets[indexHT];
    char* val = search(table, fileName);
    Worker::Index* index = worker->add_indexes();
    index->set_file_name(fileName);
    index->set_file_path(val);
 
    if (item == NULL) {//doesn't exist
        index->set_message("File doesn't exist");
        goto FINISH;
    }
    else {
        if (head == NULL && strcmp(item->fileName, fileName) == 0) {//no collsion
            table->items[indexHT] = NULL;
            free_item(item);
            table->count--;
            cout << fileName << " removed"<<endl;
            index->set_message("File Successfully removed");
            goto FINISH;
        }
        else if (head != NULL) { //collision, set head as new file
            if (strcmp(item->fileName, fileName) == 0) {
                free_item(item);
                LinkedList* node = head;
                head = head->next;
                node->next = NULL;
                table->items[indexHT] = create_item(node->item->fileName, node->item->filePath);
                free_linkedlist(node);
                table->overflow_buckets[indexHT] = head;
                cout << fileName << " removed"<<endl;
                index->set_message("File Successfully removed");
                goto FINISH;
            }
            LinkedList* curr = head;
            LinkedList* prev = NULL; 
            while (curr) {
                if (strcmp(curr->item->fileName, fileName) == 0) {
                    if (prev == NULL) {
                        // First element of the chain. removeFile the chain
                        free_linkedlist(head);
                        table->overflow_buckets[indexHT] = NULL;
                        cout << fileName << " removed"<<endl;
                        index->set_message("File Successfully removed");
                        goto FINISH;
                    }
                    else {
                        // This is somewhere in the chain
                        prev->next = curr->next;
                        curr->next = NULL;
                        free_linkedlist(curr);
                        table->overflow_buckets[indexHT] = head;
                        cout << fileName << " removed"<<endl;
                        index->set_message("File Successfully removed");
                        goto FINISH;
                    }
                }
                curr = curr->next;
                prev = curr;
            }
        }
    }
FINISH:    
    siz = worker->ByteSizeLong()+4;
    pkt = new char [siz];
    google::protobuf::io::ArrayOutputStream aos(pkt,siz);
    CodedOutputStream *coded_output = new CodedOutputStream(&aos);
    coded_output->WriteVarint32(worker->ByteSizeLong());
    worker->SerializeToCodedStream(coded_output);

    return siz;
}
 
void printSearch(Worker* worker,HashTable* table, char* fileName) {
    char* val;
    char temp[11];
    pid_t pid = getpid();
    sprintf(temp, "%d", pid);

    char pID[20] = "Query ID: ";
    strcat(pID, temp);
    
    Worker::Query* query1 = worker->add_queries();
    if ((val = search(table, fileName)) == NULL) {
        printf("%s does not exist\n", fileName);
        query1->set_result_found("Match for the query not found");
        //goto FINISH;
        return;
    }
    else {
        printf("fileName:%s, filePath:%s\n", fileName, val);
    }

/*FINISH:   
    query1->set_query(fileName);
    query1->set_query_id(pID);
    siz = worker->ByteSizeLong()+4;
    pkt = new char [siz];
    google::protobuf::io::ArrayOutputStream aos(pkt,siz);
    CodedOutputStream *coded_output = new CodedOutputStream(&aos);
    coded_output->WriteVarint32(worker->ByteSizeLong());
    worker->SerializeToCodedStream(coded_output);

    return siz;*/
}
 
void printHT(HashTable* table) {
    printf("\n---------------------------------\n");
    for (int i=0; i<table->size; i++) {
        if (table->items[i]) {
            printf("Index:%d, fileName:%s, filePath:%s", i, table->items[i]->fileName, table->items[i]->filePath);
            if (table->overflow_buckets[i]) {
                printf(" => Overflow Bucket => ");
                LinkedList* head = table->overflow_buckets[i];
                while (head) {
                    printf("fileName:%s, filePath:%s ", head->item->fileName, head->item->filePath);
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
                cout << "File: " << dataBlock->filepath << " finish indexing " << endl;
            }
            
            queue->push_empty(dataBlock);

            if (length == -1) {
                break;
            }
        }
        delete tokenizer;
        delete tokBlock;
        delete[] tokens;
        delete[] buffer;

    }
}


void searchTerm(Worker* worker, char* search_term, 
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

    Worker::Query* query1 = worker->add_queries();
    
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

    query1->set_result_found("Match for the query found");
    if(fileName == nullptr){
        query1->set_path("No file found");
    }
    else{
        cout << fileName << "file found" << endl;
    query1->set_path(fileName);
    }
    query1->set_query(search_term);
    //query1->set_query_id(pID);
    siz = worker->ByteSizeLong()+4;
    pkt = new char [siz];
    google::protobuf::io::ArrayOutputStream aos(pkt,siz);
    CodedOutputStream *coded_output = new CodedOutputStream(&aos);
    coded_output->WriteVarint32(worker->ByteSizeLong());
    worker->SerializeToCodedStream(coded_output);

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
