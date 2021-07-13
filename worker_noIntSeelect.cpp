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

using namespace std;
peer_t server;
char client_name[256];

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
  server_addr.sin_port = htons(SERVER_LISTEN_PORT);
  
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


void shutdown_properly(int code)
{
  delete_peer(&server);
  cout << "shutdown client" <<endl;
  exit(code);
}


int handle_received_message(peer_t *server,message_t *message)
{
  char read_buffer[DATA_MAXSIZE];
  cout << "Message received from server" << endl;
  print_message(message);
 
  
  // Create new message and enqueue it.
  message_t new_message;
  sprintf(read_buffer,  "%s", message->data );
  cout << "read_buffer: " << read_buffer << endl;
  prepare_message(client_name, read_buffer, &new_message);

  print_message(&new_message);
  
  
  if (peer_add_to_send(server, message) != 0) {
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
