#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <signal.h>
#include <unistd.h>
#include <sys/select.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>	
#include <iostream>

#include "message.h"

#define SIZE 1024
#define PORT 9017

#define MAX_NUM_ARGUMENTS 10
#define WHITESPACE " \t\n" 
#define MAX_MESSAGE_SIZE 250 
#define CAPACITY 1000 // Size of the Hash Table

char read_buffer[SIZE];
char client_message[SIZE];
int siz;


using namespace std;

peer_t server;
char client_name[256];

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
int printSearch( HashTable* , char* );
void printHT(HashTable*);
 
int index();
int addFile( HashTable*, char*, char*);
int removeFile( HashTable*, char*, char*);

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
  
  cout << "Connected to  " << SERVER_IPV4_ADDR << ": " << SERVER_LISTEN_PORT << endl;

  
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

int handle_received_message(peer_t *server,message_t *message)
{
  HashTable* ht = create_table(CAPACITY);
  memset(read_buffer, 0 , sizeof(read_buffer));
  memset(client_message, 0 , sizeof(client_message));
  cout << "Message received from server" << endl;
  print_message(message);
  sprintf(read_buffer,  "%s", message->data );
  
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
  shutdown_properly(2);
   
  }

  else if(strcmp(token[0], "make")==0 && strcmp(token[1], "index")==0)
  {
  
  strcpy(client_message, "made index\n");                          
  printHT(ht);
  }

  else if((strcmp(token[0],"add")==0) && (token[1]!=NULL) && (token[2] !=NULL))
  {
   addFile(ht,token[1],token[2]);  
   strcpy(client_message, "added");             
   printHT(ht);
 }

  else if((strcmp(token[0],"query")==0) && (token[1]!=NULL))
  {
    siz= printSearch(ht,token[1]); 
    strcpy(client_message, "quried");                
  }

  else if((strcmp(token[0],"remove")==0) && (token[1]!=NULL) && (token[2] !=NULL))
  {
     siz= removeFile(ht,token[1],token[2]); 
     strcpy(client_message, "remove");            
     printHT(ht);
  }
   else
   {
    strcpy(client_message, "Invalid");
    cout << client_message << endl;
   }
  // Create new message and enqueue it.
  message_t new_message;
  prepare_message(client_name, client_message, &new_message);

  print_message(&new_message);
  
 
  if (peer_add_to_send(server, &new_message) != 0) {
    cout << "Send buffer overflowed" << endl;
    return 0;
  }
  
  return 0;
}

int main(int argc, char **argv)
{
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
 
int addFile( HashTable* table, char* fileName, char* filePath) {
    HTItems* item = create_item(fileName, filePath);
   
    unsigned long indexHT = hashFunction(fileName);
  
  
 
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
        cout << fileName << " successfully added\n" <<endl;
    }
    else {
            // Scenario 1: We only need to update filePath
            if (strcmp(current_item->fileName, fileName) == 0) {
                strcpy(table->items[indexHT]->filePath, filePath);
                goto FINISH;
            }
        else {
            // Scenario 2: Collision
            handle_collision(table, indexHT, item);
            goto FINISH;
        }
    }

FINISH:
    siz = 1;
   
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

int removeFile(HashTable* table, char* fileName, char* filePath) {
    // Deletes an item from the table
    int indexHT = hashFunction(fileName);
    HTItems* item = table->items[indexHT];
    LinkedList* head = table->overflow_buckets[indexHT];
    char* val = search(table, fileName);
    
 
    if (item == NULL) {//doesn't exist
        strcpy(client_message,"File doesn't exist");
        goto FINISH;
    }
    else {
        if (head == NULL && strcmp(item->fileName, fileName) == 0) {//no collsion
            table->items[indexHT] = NULL;
            free_item(item);
            table->count--;
            cout << fileName << " removed"<<endl;
            strcpy(client_message,"File removed");
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
                strcpy(client_message,"File removed");
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
                        strcpy(client_message,"File removed");
                        goto FINISH;
                    }
                    else {
                        // This is somewhere in the chain
                        prev->next = curr->next;
                        curr->next = NULL;
                        free_linkedlist(curr);
                        table->overflow_buckets[indexHT] = head;
                        cout << fileName << " removed"<<endl;
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
    siz = 1;
    return siz;
}
 
int printSearch(HashTable* table, char* fileName) {
    char* val;
    char temp[11];
    pid_t pid = getpid();
    sprintf(temp, "%d", pid);

    char pID[20] = "Query ID: ";
    strcat(pID, temp);
    
  
    if ((val = search(table, fileName)) == NULL) {
        printf("%s does not exist\n", fileName);
        strcpy(client_message, "Match for the query not found");
        goto FINISH;
    }
    else {
        printf("fileName:%s, filePath:%s\n", fileName, val);
        strcpy(client_message, "Match for the query not found");
       
    }

FINISH:   
    siz=1;

    return siz;
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
