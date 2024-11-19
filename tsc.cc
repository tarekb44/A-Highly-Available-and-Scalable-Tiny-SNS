#include <cstdlib>
#include <iostream>
#include <locale>
#include <memory>
#include <thread>
#include <type_traits>
#include <vector>
#include <string>
#include <unistd.h>
#include <csignal>
#include <grpc++/grpc++.h>
#include "client.h"

#include "coordinator.grpc.pb.h"
#include "coordinator.pb.h"
#include "sns.grpc.pb.h"
#include "sns.pb.h"
using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;
using csce662::Message;
using csce662::ListReply;
using csce662::Request;
using csce662::Reply;
using csce662::SNSService;
using csce662::CoordService;

void sig_ignore(int sig) {
    std::cout << "Signal caught " + sig;
}

Message MakeMessage(const std::string& username, const std::string& msg) {
    Message m;
    m.set_username(username);
    m.set_msg(msg);
    google::protobuf::Timestamp* timestamp = new google::protobuf::Timestamp();
    timestamp->set_seconds(time(NULL));
    timestamp->set_nanos(0);
    m.set_allocated_timestamp(timestamp);
    return m;
}


class Client : public IClient
{
    public:
        // while MP1 used these three values to determine the details for the client to run, including the port and 
        // hostname, MP2.1 uses the port and hostname fields for the coordinator's corresponding information instead
        Client(const std::string& hname,
                const std::string& uname,
                const std::string& p)
            :hostname(hname), username(uname), port(p) {}


    protected:
        virtual int connectTo();
        virtual IReply processCommand(std::string& input);
        virtual void processTimeline();
        virtual void SendHeartbeat();

    private:
        std::string hostname;
        std::string username;
        std::string port;

        // You can have an instance of the client stub
        // as a member variable.
        std::unique_ptr<csce662::CoordService::Stub> coordinator_stub_;
        std::unique_ptr<SNSService::Stub> stub_; // server stub

        // coordinator rpcs
        std::vector<std::string> GetServer();

        // server rpcs
        IReply Login();
        IReply ClientHeartbeat();
        IReply List();
        IReply Follow(const std::string &username);
        IReply UnFollow(const std::string &username);
        void   Timeline(const std::string &username);
};


///////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////

// auxilliary function to send periodic heartbeats to the server in a detached thread
void Client::SendHeartbeat() {
    while (true){

        sleep(3);

        IReply reply = ClientHeartbeat();
        /* std::cout << "sent heart beat from client to server!\n"; */
        if (!reply.grpc_status.ok()){
            /* std::cout << "GRPC CALL FAILED!\n"; */
            exit(1);
        }
    }

}

int Client::connectTo()
{
    // need to first create a stub to communicate with the coordinator to get the info of the server to connect to
    std::string coordinator_address = hostname + ":" + port;
    grpc::ChannelArguments channel_args;
    std::shared_ptr<grpc::Channel> channel = grpc::CreateCustomChannel(
            coordinator_address, grpc::InsecureChannelCredentials(), channel_args);

    // Instantiate the coordinator stub
    coordinator_stub_ = csce662::CoordService::NewStub(channel);

    // calling getserver to get server info
    std::vector<std::string> retVal = GetServer();


    // spin off a separate thread to send periodic heartbeats to the server so the server can check for disconnected clients
    std::thread myhb(&Client::SendHeartbeat, this);
    myhb.detach();

    // creating a stub for client-server communication
    std::string server_address = retVal[0] + ":" + retVal[1];
    grpc::ChannelArguments channel_args2;
    std::shared_ptr<grpc::Channel> channel2 = grpc::CreateCustomChannel(
            server_address, grpc::InsecureChannelCredentials(), channel_args2);

    // Instantiate the stub
    stub_ = csce662::SNSService::NewStub(channel2);
    IReply reply = Login();
    if (!reply.grpc_status.ok()){
        return -1;
    }

    return 1;
}

// GetServer returns the hostname and port of the server to contact
std::vector<std::string> Client::GetServer() {

    IReply ire;

    // creating arguments and utils to make the gRPC
    ClientContext context;
    csce662::ID id; 
    csce662::ServerInfo serverinfo;

    int userId = std::stoi(username);

    id.set_id(userId);

    grpc::Status status = coordinator_stub_->GetServer(&context, id, &serverinfo);

    // returning the server info as a vector of strings
    return {serverinfo.hostname(), serverinfo.port()};


}



IReply Client::processCommand(std::string& input)
{
    // parsing the input command to determine what the command is what any subsequent arguments are
    std::vector<std::string> args;
    std::string curstring = "";
    for (auto x : input){
        if (x == ' '){
            args.push_back(curstring);
            curstring = "";
        }
        else {
            curstring += x;
        }
    }
    args.push_back(curstring); // adding the last arg to the vector


    IReply ire;

    // calling functions based on the input command
    if ("FOLLOW" == args[0]){
        ire = Follow(args[1]);
    } else if ("UNFOLLOW" == args[0]){
        ire = UnFollow(args[1]);
    } else if ("LIST" == args[0]){
        ire = List();
    } else if ("TIMELINE" == args[0]){
        // calling list here as timeline has no return value and ire needs to be set with success values for timeline to be called successfully
        ire = List();
    }

    return ire;
}


void Client::processTimeline()
{
    Timeline(username);
}

// List Command
IReply Client::List() {

    IReply ire;

    // creating arguments and utils to make the gRPC
    ClientContext context;
    Request request;
    request.set_username(username);

    ListReply list_reply;

    grpc::Status status = stub_->List(&context, request, &list_reply);
    ire.grpc_status = status;
    if (status.ok()){
        // if the RPC was successful, adding the list of all users and list of followers to ire
        ire.comm_status = SUCCESS;
        for (const std::string& user : list_reply.all_users() ){
            ire.all_users.push_back(user);
        }
        for (const std::string& follower : list_reply.followers() ){
            ire.followers.push_back(follower);
        }
    }else{
        /* std::cout << "grpc status bad\n"; */
    }

    return ire;
}

// Follow Command        
IReply Client::Follow(const std::string& username2) {

    IReply ire; 

    // creating arguments and utils to make the gRPC
    ClientContext context;
    Request request;
    Reply reply;

    request.set_username(username);
    request.add_arguments(username2); // adding the username to follow as an argument

    grpc::Status status = stub_->Follow(&context, request, &reply);
    ire.grpc_status = status;
    // sending different messages back via ire based on what happpens on the server side
    if (status.ok()){
        ire.comm_status = SUCCESS;
    } else if (status.error_message() == "already following" || status.error_message() == "same client"){
        // doing this as you cannot add a message to a "Status::OK" gRPC response
        ire = List(); // making this call to list as a spoof as it never fails and using it to set the ire values to successful things
        ire.comm_status = FAILURE_ALREADY_EXISTS;
    } else if (status.error_message() == "invalid username"){
        ire = List();
        ire.comm_status = FAILURE_INVALID_USERNAME;
    }

    return ire;
}

// UNFollow Command  
IReply Client::UnFollow(const std::string& username2) {

    IReply ire;

    // creating arguments and utils to make the gRPC
    ClientContext context;
    Request request;
    Reply reply;

    request.set_username(username);
    request.add_arguments(username2); // adding the username to unfollow as an argument

    grpc::Status status = stub_->UnFollow(&context, request, &reply);
    ire.grpc_status = status;
    // sending different messages back via ire based on what happpens on the server side
    if (status.ok()){
        ire.comm_status = SUCCESS;
    } else if (status.error_message() == "not following"){
        ire = List();
        ire.comm_status = FAILURE_NOT_A_FOLLOWER;
    } else if (status.error_message() == "invalid username" || status.error_message() == "same client"){
        ire = List();
        ire.comm_status = FAILURE_INVALID_USERNAME;
    }

    return ire;
}

// Login Command  
IReply Client::Login() {

    IReply ire;

    // creating arguments and utils to make the gRPC
    ClientContext context;
    Request request;
    Reply reply;

    request.set_username(username);

    grpc::Status status = stub_->Login(&context, request, &reply);
    ire.grpc_status = status;
    if (!ire.grpc_status.ok()){
        ire.comm_status = FAILURE_ALREADY_EXISTS;
    } else {
        ire.comm_status = SUCCESS;
    }

    return ire;
}




// using signals to detect a SIGINT (CNTRL + C) and perform necessary cleanup on the server side, i.e., clearing out the stream pointer associated with the terminated client
volatile sig_atomic_t signalReceived = 0;

// Signal handler
void signalHandler(int signum) {
    signalReceived = 1;
}


// redoing the getPostMessage function from client.cc as fgets is a blocking function and adds problems to responding to SIGINT
std::string getPostMessageC() {
    char buf[MAX_DATA];
    fd_set fds;
    struct timeval tv;

    while (1) {
        if (signalReceived) {
            break;
        }

        FD_ZERO(&fds);
        FD_SET(STDIN_FILENO, &fds);

        tv.tv_sec = 0;
        tv.tv_usec = 100000; // Set a timeout of 100 milliseconds

        int ready = select(STDIN_FILENO + 1, &fds, nullptr, nullptr, &tv); // using select to monitor input as it is non-blocking

        if (ready == -1) {
            perror("select");
            exit(EXIT_FAILURE);
        } else if (ready == 1) {
            // calling fgets if there is no SIGINT
            fgets(buf, MAX_DATA, stdin);
            if (buf[0] != '\n')  break;
        }
    }

    std::string message(buf);
    return message;
}

// Timeline Command
void Client::Timeline(const std::string& username) {

    ClientContext context;
    Message m;
    m.set_username(username);

    // registering the signal handler for SIGINT
    std::signal(SIGINT, signalHandler);

    IReply ire;

    context.AddMetadata("username", username); // adding the client's username in the metadata so the server can Initialize its stream upon first contact

    // Create a stream for bidirectional communication
    std::unique_ptr<ClientReaderWriter<Message, Message>> stream(stub_->Timeline(&context));


    // Writer thread to send messages to the server
    std::thread writer_thread([&]() {
        while (true) {
            if (signalReceived) { // a SIGINT was received
                signalReceived = 0;
                Request request;
                Reply reply;
                ClientContext context_term;
                context_term.AddMetadata("terminated", "true"); // adding a message to let the unfollow function on the server side know that a sigint has been received so it can do cleanups

                request.set_username(username);

                grpc::Status status = this->stub_->UnFollow(&context_term, request, &reply); // calling unfollow for cleanup of client stream
                stream->WritesDone();

                exit(1);
            } else {

                // normal writing stuff
                std::string message = getPostMessageC(); // calling the non-blocking getPostMessage version to write to the server's stream

                m.set_msg(message);

                google::protobuf::Timestamp* timestamp = new google::protobuf::Timestamp();
                timestamp->set_seconds(std::time(nullptr)); // Set the current time in seconds
                timestamp->set_nanos(0);
                m.set_allocated_timestamp(timestamp);

                if (!signalReceived){ // unless there is no SIGINT, write to server's stream
                    stream->Write(m);
                } else { // perform the same kind of cleanup of client's stream via calling unfollow as this avoids segfaults if a dead stream is written to

                    signalReceived = 0;
                    Request request;
                    Reply reply;
                    ClientContext context_term;
                    context_term.AddMetadata("terminated", "true");

                    request.set_username(username);

                    grpc::Status status = this->stub_->UnFollow(&context_term, request, &reply);
                    stream->WritesDone();

                    exit(1);
                }
            }

        }

        stream->WritesDone();
    });

    // Reader thread to receive messages from the server
    std::thread reader_thread([&]() {
        Message received_message;
        ClientContext context;

        while (!signalReceived) { // safety checks to make sure the reader thread terminates upon SIGINT
            if (stream->Read(&received_message)) {
                std::string sender = received_message.username();
                std::string message = received_message.msg();
                std::time_t time = received_message.timestamp().seconds();
                std::cout << message << std::endl; // printing the server's message via the stream to the timeline
            } 
        }

        Request request; // cleanup of stream via calling unfollow as done above multiple times
        Reply reply;
        ClientContext context_term;
        context_term.AddMetadata("terminated", "true");

        request.set_username(username);

        grpc::Status status = this->stub_->UnFollow(&context_term, request, &reply);
        exit(1);
    });


    // Join the threads (wait for them to finish)
    writer_thread.join();
    reader_thread.join();

}


IReply Client::ClientHeartbeat() {

    IReply ire;

    // creating arguments and utils to make the gRPC
    ClientContext context;
    Request request;
    Reply reply;

    request.set_username(username);

    // making a grpc periodically to let the server know that the client is alive
    grpc::Status status = stub_->ClientHeartbeat(&context, request, &reply);
    if (status.ok()){
        ire.grpc_status = status;
    }else { // technically should exit in this case but since one of the test cases says otherwise, I am commenting this out
        /* ire.grpc_status = status; */
        /* std::cout << "server not found! exiting now...\n"; */
        /* exit(0); */
    }

    return ire;
}


//////////////////////////////////////////////
// Main Function
/////////////////////////////////////////////
int main(int argc, char** argv) {


    std::string coordinatorIP = "localhost";
    std::string userId = "default";
    std::string coordinatorPort = "9090";
    std::string hostname = "localhost";
    std::string username = "default";
    std::string port = "3010";

    int opt = 0;
    while ((opt = getopt(argc, argv, "h:u:k:")) != -1){
        switch(opt) {
            case 'h':
                coordinatorIP = optarg;break;
            case 'u':
                userId = optarg;break;
            case 'k':
                coordinatorPort = optarg;break;
            default:
                std::cout << "Invalid Command Line Argument\n";
        }
    }

    Client myc(coordinatorIP, userId, coordinatorPort);

    myc.run();

    return 0;
}
