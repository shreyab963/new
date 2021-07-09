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

#define SIZE 1000
#define PORT 9018

char* pkt;
int siz;

using namespace google::protobuf::io;

using namespace std;


int index(Worker*);
int search(Worker*,char*);
int addFile(Worker*, char*, char*);
int removeFile(Worker*, char*, char*);

int main(int argv, char** argc){

        WorkersLog workers_log;
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
     
        recv(hsock, server_message,1000,0);
        cout << server_message <<endl;

        while( (bytecount = recv(hsock , server_message , SIZE, 0)) > 0 )
        {
            cout << server_message << endl;
            char client_message[SIZE] = "NULL";

            if(strcmp(server_message, "shutdown")==0)
            {
                strcpy(client_message, "Shutting down client\n");
                if( (bytecount=send(hsock, client_message ,SIZE,0))== -1 ) 
                {
                        cerr << "Error sending shutdown message\n "  << endl;
                        goto FINISH;
                }
                cout << client_message << endl;
                return 0;
            }

            else if(strcmp(server_message, "make index")==0)
            {
                siz= index(workers_log.add_workers());             
                if( (bytecount=send(hsock,(void*)pkt,siz,0))== -1 ) 
                {
                        cerr << "Error sending data\n " << endl;   
                }
            }

            else if(strcmp(server_message, "add file")==0)
            {
               siz= addFile(workers_log.add_workers(),server_message,server_message);             
                if( (bytecount=send(hsock,(void*)pkt,siz,0))== -1 ) 
                {
                        cerr << "Error sending data\n " << endl;   
                }
            }

            else if(strcmp(server_message,"query")==0)
            {
                siz= search(workers_log.add_workers(),server_message); 
                if( (bytecount=send(hsock,(void*)pkt,siz,0))== -1 ) 
                {
                        cerr << "Error sending data\n " << endl;   
                }
            }

            else
            {
                strcpy(client_message, "Invalid command");
                send(hsock, client_message,SIZE,0);
                cout << client_message <<endl;
            }
        }
        
        delete pkt;
FINISH:
        close(hsock);
}

int index(Worker* worker)
{
    worker->set_worker_name("mystic1");
    worker->set_worker_id(1);
   /* Worker::Index* index = worker->add_indexes();
    index->set_indexed(true);*/
    siz = worker->ByteSizeLong()+4;
    pkt = new char [siz];
    google::protobuf::io::ArrayOutputStream aos(pkt,siz);
    CodedOutputStream *coded_output = new CodedOutputStream(&aos);
    coded_output->WriteVarint32(worker->ByteSizeLong());
    worker->SerializeToCodedStream(coded_output);
return siz;
}

int addFile(Worker* worker, char* fileName, char* filePath)
{
    worker->set_worker_name("mystic1");
    worker->set_worker_id(1);
   /* Worker::Index* index = worker->add_indexes();
    index->set_file_added(false);
    index->set_file_name("data.txt");
    index->set_file_path("/home/data.txt");*/

    siz = worker->ByteSizeLong()+4;
    pkt = new char [siz];
    google::protobuf::io::ArrayOutputStream aos(pkt,siz);
    CodedOutputStream *coded_output = new CodedOutputStream(&aos);
    coded_output->WriteVarint32(worker->ByteSizeLong());
    worker->SerializeToCodedStream(coded_output); 

    return siz;
}

int removeFile(Worker* worker, char* fileName, char* filePath)
{
    worker->set_worker_name("mystic1");
    worker->set_worker_id(1);
   /* Worker::Index* index = worker->add_indexes();
    index->set_file_removed(true);
    index->set_file_name("data.txt");
    index->set_file_path("/home/data.txt");*/

    siz = worker->ByteSizeLong()+4;
    pkt = new char [siz];
    google::protobuf::io::ArrayOutputStream aos(pkt,siz);
    CodedOutputStream *coded_output = new CodedOutputStream(&aos);
    coded_output->WriteVarint32(worker->ByteSizeLong());
    worker->SerializeToCodedStream(coded_output);

    return siz;
}

int search(Worker* worker,char* query)
{
    worker->set_worker_name("mystic");
    worker->set_worker_id(2);
    /*Worker::Query* query1 = worker->add_queries();
    query1->set_result_found(true);
    query1->set_query("this is the query");
    query1->set_query_id(123);
    query1->set_path("/home/data.txt");*/

    char* path = new char[100];
    strcpy(path, "home/path.txt");
    cout << "i'm here" <<endl;

    siz = worker->ByteSizeLong()+4;
    pkt = new char [siz];
    google::protobuf::io::ArrayOutputStream aos(pkt,siz);
    CodedOutputStream *coded_output = new CodedOutputStream(&aos);
    coded_output->WriteVarint32(worker->ByteSizeLong());
    worker->SerializeToCodedStream(coded_output);

return siz;
    
}
int copyFile(Worker* worker, char* filefrom, char* filePathF, char* fileTo, char* filePathT)
{
     
    Worker::Index* index = worker->add_indexes();
    index->set_file_removed(true);
    index->set_file_name("data.txt");
    index->set_file_path("/home/data.txt");

    siz = worker->ByteSizeLong()+4;
    pkt = new char [siz];
    google::protobuf::io::ArrayOutputStream aos(pkt,siz);
    CodedOutputStream *coded_output = new CodedOutputStream(&aos);
    coded_output->WriteVarint32(worker->ByteSizeLong());
    worker->SerializeToCodedStream(coded_output);

    return siz;
}
int moveFile(Worker* worker, char* filefrom, char* filePathF, char* fileTo, char* filePathT)
{
    Worker::Index* index = worker->add_indexes();
    index->set_file_removed(true);
    index->set_file_name("data.txt");
    index->set_file_path("/home/data.txt");

    siz = worker->ByteSizeLong()+4;
    pkt = new char [siz];
    google::protobuf::io::ArrayOutputStream aos(pkt,siz);
    CodedOutputStream *coded_output = new CodedOutputStream(&aos);
    coded_output->WriteVarint32(worker->ByteSizeLong());
    worker->SerializeToCodedStream(coded_output);

    return siz;
}
