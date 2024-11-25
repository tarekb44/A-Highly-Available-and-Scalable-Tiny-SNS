/*
 *
 * Copyright 2015, Google Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include <filesystem>
#include <semaphore.h>  // For semaphores (sem_t, sem_open, etc.)
#include <fcntl.h>  
#include <cstddef>
#include <cstdlib>
#include <thread>
#include <cstdio>
#include <ctime>
#include <csignal>
#include <jsoncpp/json/json.h>
#include <unordered_set>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>

#include <fstream>
#include <iostream>
#include <list>
#include <memory>
#include <string>
#include <stdlib.h>
#include <unistd.h>
#include <unordered_map>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include<glog/logging.h>
#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity); 

#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <amqp_framing.h>

#include "sns.grpc.pb.h"
#include "coordinator.grpc.pb.h"
#include "coordinator.pb.h"
#include "client.h"

#include "master_slave.grpc.pb.h"
#include "master_slave.pb.h"

using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;
using csce662::Message;
using csce662::ListReply;
using csce662::Request;
using csce662::Reply;
using csce662::SNSService;
using csce662::CoordService;
using csce662::ServerInfo;
using csce662::Confirmation;

using grpc::Channel;
using grpc::ClientContext;
using csce662::MasterSlaveService;
using csce662::ClientUpdate;
using csce662::Ack;


std::string globalClusterid;
struct Client {
    std::string username;
    bool connected = true;
    int following_file_size = 0;
    std::vector<Client*> client_followers;
    std::vector<Client*> client_following;
    ServerReaderWriter<Message, Message>* stream = 0;
    // adding these two new variables below to monitor client heartbeats
    std::time_t last_heartbeat;
    bool missed_heartbeat = false;
    bool operator==(const Client& c1) const{
        return (username == c1.username);
    }
};

amqp_connection_state_t setupRabbitMQConnection() {
    amqp_connection_state_t conn = amqp_new_connection();
    amqp_socket_t *socket = amqp_tcp_socket_new(conn);
    if (!socket) {
        std::cerr << "Creating TCP socket failed" << std::endl;
        exit(1);
    }

    int status = amqp_socket_open(socket, "localhost", 5672);
    if (status) {
        std::cerr << "Opening TCP socket failed" << std::endl;
        exit(1);
    }

    amqp_rpc_reply_t loginReply = amqp_login(conn, "/", 0, 131072, 0,
                                             AMQP_SASL_METHOD_PLAIN, "guest", "guest");
    if (loginReply.reply_type != AMQP_RESPONSE_NORMAL) {
        std::cerr << "Logging in to RabbitMQ failed" << std::endl;
        exit(1);
    }

    amqp_channel_open(conn, 1);
    amqp_rpc_reply_t channelReply = amqp_get_rpc_reply(conn);
    if (channelReply.reply_type != AMQP_RESPONSE_NORMAL) {
        std::cerr << "Opening channel failed" << std::endl;
        exit(1);
    }

    return conn;
}

void consumeAndCollectUsers(amqp_connection_state_t conn, const std::string &queueName, std::vector<std::string> &users) {
    amqp_queue_declare(conn, 1, amqp_cstring_bytes(queueName.c_str()),
                       0, 
                       0,
                       0, 
                       1,
                       amqp_empty_table);

    // Start consuming messages
    amqp_basic_consume(conn, 1, amqp_cstring_bytes(queueName.c_str()),
                       amqp_empty_bytes,
                       0, 
                       1,
                       0,
                       amqp_empty_table);

    amqp_rpc_reply_t res;
    amqp_envelope_t envelope;

    while (true) {
        amqp_maybe_release_buffers(conn);
        struct timeval timeout = {1, 0}; // 1-second timeout

        res = amqp_consume_message(conn, &envelope, &timeout, 0);
        if (AMQP_RESPONSE_NORMAL != res.reply_type) {
            if (res.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION &&
                res.library_error == AMQP_STATUS_TIMEOUT) {
                break; // No more messages in the queue
            } else {
                std::cerr << "Error consuming message" << std::endl;
                break;
            }
        }

        std::string message((char *)envelope.message.body.bytes, envelope.message.body.len);

        Json::Reader reader;
        Json::Value root;
        if (reader.parse(message, root)) {
            const Json::Value usersJson = root["users"];
            for (const auto &user : usersJson) {
                users.push_back(user.asString());
            }
        } else {
            std::cerr << "Failed to parse message: " << message << std::endl;
        }

        amqp_destroy_envelope(&envelope);
    }
}

void checkHeartbeat();
std::time_t getTimeNow();

std::unique_ptr<csce662::CoordService::Stub> coordinator_stub_;
std::unique_ptr<MasterSlaveService::Stub> slave_stub_;

std::string clusterId;
std::string serverId;
bool isMaster = false;

// coordinator rpcs
IReply Heartbeat(std::string clusterId, std::string serverId, std::string hostname, std::string port);


/* std::vector<Client*> client_db; */

// using an unordered map to store clients rather than a vector as this allows for O(1) accessing and O(1) insertion
std::unordered_map<std::string, Client*> client_db;

// util function for checking if a client exists in the client_db and fetching it if it does
Client* getClient(std::string username){
    auto it = client_db.find(username);

    if (it != client_db.end()) {
        return client_db[username];
    } else {
        return NULL;
    }

}

class SlaveServiceImpl final : public MasterSlaveService::Service {
 public:
  Status ForwardUpdate(ServerContext* context, const ClientUpdate* request, Ack* response) override {
    std::string username = request->username();
    std::string message = request->message();

    std::ofstream outfile("./cluster_" + clusterId + "/2/" + username + ".txt", std::ios_base::app);
    outfile << message << std::endl;
    outfile.close();

    response->set_success(true);
    return Status::OK;
  }
};

void logClientDB() {
    std::cout << "Current state of client_db:" << std::endl;
    for (const auto& pair : client_db) {
        Client* client = pair.second;
        std::cout << "Client: " << client->username << std::endl;
        std::cout << "  Following: ";
        for (const auto& following : client->client_following) {
            std::cout << following->username << " ";
        }
        std::cout << std::endl;

        std::cout << "  Followers: ";
        for (const auto& follower : client->client_followers) {
            std::cout << follower->username << " ";
        }
        std::cout << std::endl;
    }
}

// this method is always listening and is always updating client_db
void continuouslyConsumeMessages(amqp_connection_state_t conn, amqp_channel_t channel, const std::string& queueName) {
    // consume from teh hello_queue for users
    amqp_basic_consume(
        conn, 
        channel, 
        amqp_cstring_bytes(queueName.c_str()), 
        amqp_empty_bytes, 
        0, 
        0,
        0, 
        amqp_empty_table
    );

    while (true) {
        amqp_rpc_reply_t res;
        amqp_envelope_t envelope;

        amqp_maybe_release_buffers(conn);

        // consume the message
        res = amqp_consume_message(conn, &envelope, NULL, 0);

        if (AMQP_RESPONSE_NORMAL == res.reply_type) {
            std::string message(static_cast<char*>(envelope.message.body.bytes), envelope.message.body.len);
            std::istringstream iss(message);
            std::string username, followings;

            if (std::getline(iss, username, ':')) {
                username.erase(username.find_last_not_of(" \n\r\t") + 1); // Trim whitespace

                if (username.empty() || username == "None") {
                    std::cerr << "Invalid username detected in message: " << message << std::endl;
                    amqp_basic_ack(conn, channel, envelope.delivery_tag, 0);
                    amqp_destroy_envelope(&envelope);
                    continue;
                }

                // create a new client
                Client* client = getClient(username);
                if (!client) {
                    client = new Client();
                    client->username = username;
                    client->connected = true;
                    client->last_heartbeat = getTimeNow();
                    client_db[username] = client;
                }

                if (std::getline(iss, followings)) {
                    followings.erase(followings.find_last_not_of(" \n\r\t") + 1); // Trim whitespace
                    std::istringstream followingStream(followings);
                    std::string followingUsername;

                    while (followingStream >> followingUsername) {
                        if (followingUsername.empty() || followingUsername == "None") {
                            std::cerr << "Invalid following username detected: " << followingUsername << std::endl;
                            continue;
                        }

                        Client* followingClient = getClient(followingUsername);
                        if (!followingClient) {
                            followingClient = new Client();
                            followingClient->username = followingUsername;
                            followingClient->connected = true;
                            followingClient->last_heartbeat = getTimeNow();
                            client_db[followingUsername] = followingClient;
                        }

                        if (std::find(client->client_following.begin(), client->client_following.end(), followingClient) == client->client_following.end()) {
                            client->client_following.push_back(followingClient);
                        }

                        if (std::find(followingClient->client_followers.begin(), followingClient->client_followers.end(), client) == followingClient->client_followers.end()) {
                            followingClient->client_followers.push_back(client);
                        }
                    }
                }
            }

            // logClientDB();

            amqp_basic_ack(conn, channel, envelope.delivery_tag, 0);
            amqp_destroy_envelope(&envelope);
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
}

void initializeRabbitMQAndContinuouslyConsume() {
    const std::string hostname = "localhost";
    const int port = 5672;
    const std::string queueName = "hello_queue";

    amqp_connection_state_t conn = amqp_new_connection();
    amqp_socket_t* socket = amqp_tcp_socket_new(conn);

    if (!socket || amqp_socket_open(socket, hostname.c_str(), port)) {
        throw std::runtime_error("Failed to open RabbitMQ connection.");
    }

    amqp_login(conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, "guest", "guest");
    amqp_channel_open(conn, 1);
    amqp_get_rpc_reply(conn);

    try {
        continuouslyConsumeMessages(conn, 1, queueName);
    } catch (const std::exception& ex) {
        std::cerr << "Exception in continuous consumption: " << ex.what() << std::endl;
    }

    // Cleanup
    amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS);
    amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(conn);
}


class SNSServiceImpl final : public SNSService::Service {

    Status ClientHeartbeat(ServerContext* context, const Request* request, Reply* reply) override {

        std::string username = request->username();

        /* std::cout << "got a heartbeat from client: " << username << std::endl; */
        Client* c = getClient(username);
        if (c != NULL){
            c->last_heartbeat = getTimeNow();

        } else {
            std::cout << "client was not found, for some reason!\n";
            return Status::OK;
        }

        return Status::OK;
    }

    Status List(ServerContext* context, const Request* request, ListReply* list_reply) override {

        // add all known clients to the all_users vector
        for (const auto& pair : client_db){
            list_reply->add_all_users(pair.first);
        }

        std::string username = request->username();

        // add all the followers of the client to the folowers vector
        Client* c = getClient(username);
        if (c != NULL){
            for (Client* x : c->client_followers){
                list_reply->add_followers(x->username);
            }

        } else {
            return Status::CANCELLED;
        }

        return Status::OK;
    }
        
    Status Follow(ServerContext* context, const Request* request, Reply* reply) override {

        std::string u1 = request->username();
        std::string u2 = request->arguments(0);
        Client* c1 = getClient(u1);
        Client* c2 = getClient(u2);

        if (c1 == nullptr || c2 == nullptr) { // either of the clients don't exist
            return Status(grpc::CANCELLED, "invalid username");
        }

        if (c1 == c2) { // if a client is asked to follow itself
            return Status(grpc::CANCELLED, "same client");
        }

        // Check if the client to follow is already being followed
        bool isAlreadyFollowing = std::find(c1->client_following.begin(), c1->client_following.end(), c2) != c1->client_following.end();

        if (isAlreadyFollowing) {
            return Status(grpc::CANCELLED, "already following");
        }

        // Add the clients to each other's relevant vector
        c1->client_following.push_back(c2);
        c2->client_followers.push_back(c1);

        // Ensure cluster directory structure
        int clusterID = std::stoi(clusterId); // Assuming clusterId is accessible globally
        std::string clusterDirectory = "./cluster_" + std::to_string(clusterID);
        std::string clusterSubdirectory = "1"; // Adjust as needed for cluster-specific subdirectories
        std::string clusterPath = clusterDirectory + "/" + clusterSubdirectory;
        std::string usersFile = clusterPath + "/all_users.txt";
        std::string semName = "/" + std::to_string(clusterID) + "_" + clusterSubdirectory + "_all_users.txt";

        // Create directories if they do not exist
        std::filesystem::create_directories(clusterPath);

        sem_t* fileSem = sem_open(semName.c_str(), O_CREAT, 0644, 1);
        if (fileSem == SEM_FAILED) {
            return Status(grpc::INTERNAL, "Semaphore creation failed");
        }

        sem_wait(fileSem);
        {
            // Open the file and update the user list
            std::ofstream file(usersFile, std::ios::trunc);
            if (file.is_open()) {
                for (const auto& pair : client_db) {
                    Client* client = pair.second;
                    file << client->username << ": ";
                    for (const auto& following : client->client_following) {
                        file << following->username << " ";
                    }
                    file << std::endl;
                }
                file.close();
            } else {
                sem_post(fileSem);
                sem_close(fileSem);
                return Status(grpc::INTERNAL, "Failed to open users file");
            }
        }
        sem_post(fileSem);
        sem_close(fileSem);

        std::cout << u1 << " is now following " << u2 << std::endl;

        return Status::OK; 
    }

    Status UnFollow(ServerContext* context, const Request* request, Reply* reply) override {

        std::string username = request->username();
        const std::multimap<grpc::string_ref, grpc::string_ref>& metadata = context->client_metadata();

        auto it = metadata.find("terminated");
        if (it != metadata.end()) {
            std::string customValue(it->second.data(), it->second.length());
            std::string termStatus = customValue;

            if (termStatus == "true") {
                Client* c = getClient(username);
                if (c != nullptr) {
                    c->last_heartbeat = getTimeNow();
                    c->connected = false;
                    c->stream = nullptr;
                }
                return Status::OK;
            }
        }

        std::string u1 = request->username();
        std::string u2 = request->arguments(0);
        Client* c1 = getClient(u1);
        Client* c2 = getClient(u2);

        if (c1 == nullptr || c2 == nullptr) {
            return Status(grpc::CANCELLED, "invalid username");
        }

        if (c1 == c2) {
            return Status(grpc::CANCELLED, "same client");
        }

        // Find and remove c2 from c1's following list
        auto it1 = std::find(c1->client_following.begin(), c1->client_following.end(), c2);
        if (it1 != c1->client_following.end()) {
            c1->client_following.erase(it1);
        } else {
            return Status(grpc::CANCELLED, "not following");
        }

        // Find and remove c1 from c2's followers list
        auto it2 = std::find(c2->client_followers.begin(), c2->client_followers.end(), c1);
        if (it2 != c2->client_followers.end()) {
            c2->client_followers.erase(it2);
        }

        // Update cluster files for both users
        int clusterID = std::stoi(clusterId);  // Assuming clusterId is accessible globally
        std::string clusterSubdirectory = "1";  // Adjust as needed for cluster-specific subdirectories
        std::string usersFile = "./cluster_" + std::to_string(clusterID) + "/" + clusterSubdirectory + "/all_users.txt";
        std::string semName = "/" + std::to_string(clusterID) + "_" + clusterSubdirectory + "_all_users.txt";

        sem_t* fileSem = sem_open(semName.c_str(), O_CREAT, 0644, 1);
        if (fileSem == SEM_FAILED) {
            return Status(grpc::INTERNAL, "Semaphore creation failed");
        }

        sem_wait(fileSem);
        {
            // Open the file and write updated user lists
            std::ofstream file(usersFile, std::ios::trunc);
            if (file.is_open()) {
                for (const auto& pair : client_db) {
                    Client* client = pair.second;
                    file << client->username << ": ";
                    for (const auto& following : client->client_following) {
                        file << following->username << " ";
                    }
                    file << std::endl;
                }
                file.close();
            } else {
                sem_post(fileSem);
                sem_close(fileSem);
                return Status(grpc::INTERNAL, "Failed to open users file");
            }
        }
        sem_post(fileSem);
        sem_close(fileSem);

        return Status::OK;
    }

    // RPC Login
    Status Login(ServerContext* context, const Request* request, Reply* reply) override {

        std::string username = request->username();

        Client* c = getClient(username);
        bool isNewClient = false;

        // Check if the client exists
        if (c != nullptr) {
            // If the client is already active
            if (c->connected) {
                c->missed_heartbeat = false;
                return Status::CANCELLED;
            } else {
                // Client was previously inactive, mark as connected
                c->connected = true;
                c->last_heartbeat = getTimeNow();
                c->missed_heartbeat = false;
            }
        } else {
            // Create a new client if it doesn't exist
            Client* newc = new Client();
            newc->username = username;
            newc->connected = true;
            newc->last_heartbeat = getTimeNow();
            newc->missed_heartbeat = false;
            client_db[username] = newc;
            isNewClient = true;
        }

        // Ensure cluster directory structure
        int clusterID = std::stoi(clusterId);  // Assuming clusterId is accessible globally
        std::string clusterDirectory = "./cluster_" + std::to_string(clusterID);
        std::string clusterSubdirectory = "1";  // Adjust as needed for cluster-specific subdirectories
        std::string clusterPath = clusterDirectory + "/" + clusterSubdirectory;
        std::string usersFile = clusterPath + "/all_users.txt";
        std::string semName = "/" + std::to_string(clusterID) + "_" + clusterSubdirectory + "_all_users.txt";

        // Create directories if they do not exist
        std::filesystem::create_directories(clusterPath);

        sem_t* fileSem = sem_open(semName.c_str(), O_CREAT, 0644, 1);
        if (fileSem == SEM_FAILED) {
            return Status(grpc::INTERNAL, "Semaphore creation failed");
        }

        sem_wait(fileSem);
        {
            // Open the file and update the user list
            std::ofstream file(usersFile, std::ios::trunc);
            if (file.is_open()) {
                for (const auto& pair : client_db) {
                    Client* client = pair.second;
                    file << client->username << ": ";
                    for (const auto& following : client->client_following) {
                        file << following->username << " ";
                    }
                    file << std::endl;
                }
                file.close();
            } else {
                sem_post(fileSem);
                sem_close(fileSem);
                return Status(grpc::INTERNAL, "Failed to open users file");
            }
        }
        sem_post(fileSem);
        sem_close(fileSem);

        if (isNewClient) {
            std::cout << "New client registered: " << username << std::endl;
        } else {
            std::cout << "Client reconnected: " << username << std::endl;
        }

        return Status::OK;
    }

    const int MAX_MESSAGES = 20;

    Status Timeline(ServerContext* context, ServerReaderWriter<Message, Message>* stream) override {

        // Initialize variables important for persisting timelines on the disk
        Message m;
        Client* c;
        std::string u;
        std::vector<std::string> latestMessages;
        std::vector<std::string> allMessages;
        bool firstTimelineStream = true;

        // Fetch metadata to get the username of the current client
        const auto& metadata = context->client_metadata();
        auto it = metadata.find("username");
        if (it != metadata.end()) {
            std::string customValue(it->second.data(), it->second.length());
            u = customValue;  // Username from metadata
            c = getClient(u);
            if (c) {
                c->stream = stream;  // Associate this stream with the client
                std::cout << "Timeline RPC invoked for user: " << u << std::endl;
            } else {
                std::cerr << "Client not found for username: " << u << std::endl;
            }
        } else {
            std::cerr << "No username found in metadata." << std::endl;
        }

        // If this is the first time the client is logging in, send the latest messages
        if (firstTimelineStream && c != nullptr) {
            // Read latest messages from the following file
            std::ifstream followingFile(u + "_following.txt");
            if (followingFile.is_open()) {
                std::string line;
                while (std::getline(followingFile, line)) {
                    allMessages.push_back(line);
                }

                // Determine the starting index for retrieving latest messages
                int startIndex = std::max(0, static_cast<int>(allMessages.size()) - MAX_MESSAGES);

                // Retrieve the latest messages
                for (int i = startIndex; i < allMessages.size(); ++i) {
                    latestMessages.push_back(allMessages[i]);
                }
                std::reverse(latestMessages.begin(), latestMessages.end()); // Match assignment order
                followingFile.close();
            } else {
                std::cerr << "Failed to open following file for user: " << u << std::endl;
            }

            // Send the latest messages to the client
            for (const std::string& msg : latestMessages) {
                Message latestMessage;
                latestMessage.set_msg(msg + "\n");
                std::cout << "Sending initial message to " << u << ": " << msg << std::endl;
                if (!stream->Write(latestMessage)) {
                    std::cerr << "Failed to write initial message to client: " << u << std::endl;
                }
            }
            firstTimelineStream = false;
        }

        // Read messages from the client
        while (stream->Read(&m)) {
            if (c != nullptr) {

                std::time_t timestamp_seconds = m.timestamp().seconds();
                std::tm* timestamp_tm = std::gmtime(&timestamp_seconds);

                char time_str[50];
                std::strftime(time_str, sizeof(time_str), "%a %b %d %T %Y", timestamp_tm);

                std::string ffo = u + '(' + time_str + ')' + " >> " + m.msg();
                std::cout << "Received message from " << u << ": " << m.msg() << std::endl;

                // create the timeline path
                std::string timelineFilePath = "./cluster_" + clusterId + "/1/" + u + "_timeline.txt";
                std::cout << "Writing to timeline file: " << timelineFilePath << std::endl;

                // write tto the timeline path
                std::ofstream timelineFile(timelineFilePath, std::ios_base::app);
                if (timelineFile.is_open()) {
                    timelineFile << ffo << std::endl;
                    timelineFile.close();
                    std::cout << "Successfully wrote to timeline file: " << ffo << std::endl;
                } else {
                    std::cerr << "Failed to open timeline file: " << timelineFilePath << std::endl;
                }

                // update the slave too
                if (isMaster && slave_stub_) {
                    ClientUpdate update;
                    update.set_username(u);
                    update.set_message(ffo);

                    ClientContext slave_context;
                    Ack ack;
                    Status status = slave_stub_->ForwardUpdate(&slave_context, update, &ack);

                    if (!status.ok()) {
                        std::cout << "Failed to forward update to Slave: " << status.error_message() << std::endl;
                    } else {
                        std::cout << "Successfully forwarded update to Slave for user: " << u << std::endl;
                    }
                }

                // since c->client_followers is updated from the syncrhonizer
                // stream this to its rabbitmq queue
                for (Client* follower : c->client_followers) {
                    std::cout << "Notifying follower: " << follower->username << std::endl;

                    std::string followerQueueName = "timeline_queue_" + follower->username;

                    try {
                        amqp_connection_state_t conn = amqp_new_connection();
                        amqp_socket_t* socket = amqp_tcp_socket_new(conn);
                        if (!socket) {
                            std::cerr << "Failed to create RabbitMQ TCP socket." << std::endl;
                            continue;
                        }

                        if (amqp_socket_open(socket, "localhost", 5672)) {
                            std::cerr << "Failed to open RabbitMQ socket to localhost:5672" << std::endl;
                            continue;
                        }

                        amqp_rpc_reply_t loginReply = amqp_login(conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, "guest", "guest");
                        if (loginReply.reply_type != AMQP_RESPONSE_NORMAL) {
                            std::cerr << "Failed to log in to RabbitMQ." << std::endl;
                            continue;
                        }

                        amqp_channel_open(conn, 1);
                        amqp_rpc_reply_t openChannelReply = amqp_get_rpc_reply(conn);
                        if (openChannelReply.reply_type != AMQP_RESPONSE_NORMAL) {
                            std::cerr << "Failed to open RabbitMQ channel." << std::endl;
                            continue;
                        }

                        amqp_queue_declare_ok_t* queueDeclareReply = amqp_queue_declare(
                            conn, 1, amqp_cstring_bytes(followerQueueName.c_str()), 0, 1, 0, 0, amqp_empty_table
                        );

                        if (!queueDeclareReply) {
                            std::cerr << "Failed to declare RabbitMQ queue: " << followerQueueName << std::endl;
                            continue;
                        }

                        std::cout << "RabbitMQ: Successfully declared queue " << followerQueueName << std::endl;

                        // publish the timeline message from the client
                        int publishResult = amqp_basic_publish(
                            conn, 1, amqp_cstring_bytes(""), amqp_cstring_bytes(followerQueueName.c_str()),
                            0, 0, nullptr, amqp_cstring_bytes(ffo.c_str())
                        );

                        if (publishResult < 0) {
                            std::cerr << "Failed to publish message to RabbitMQ queue: " << followerQueueName << std::endl;
                        } else {
                            std::cout << "RabbitMQ: Successfully published message to " << followerQueueName << ": " << ffo << std::endl;
                        }

                        amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS);
                        amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
                        amqp_destroy_connection(conn);

                    } catch (const std::exception& e) {
                        std::cerr << "RabbitMQ: Exception encountered: " << e.what() << std::endl;
                    }
                }

                // this is used to update the follower path of the client 
                for (Client* follower : c->client_followers) {
                    std::string follower_filepath = "./cluster_" + clusterId + "/1/" + follower->username + "_following.txt";
                    
                    if (std::filesystem::exists(follower_filepath)) {
                        std::ofstream followerFile(follower_filepath, std::ios_base::app);
                        if (followerFile.is_open()) {
                            followerFile << ffo << std::endl;
                            followerFile.close();
                            std::cout << "Updated following file for follower: " << follower->username << std::endl;

                            if (follower->stream != nullptr) {
                                Message updateMessage;
                                updateMessage.set_msg(ffo + "\n");  // Add a newline for readability
                                std::cout << "Sending update message to follower " << follower->username << ": " << ffo << std::endl;
                                
                                // attempt to write the message to the follower's stream
                                if (!follower->stream->Write(updateMessage)) {
                                    std::cerr << "Failed to write update message to follower: " << follower->username << std::endl;
                                }
                            } else {
                                std::cerr << "Follower stream is null for user: " << follower->username << std::endl;
                            }
                        } else {
                            std::cerr << "Failed to open following file for follower: " << follower->username << std::endl;
                        }
                    } else {
                        std::cerr << "Following file does not exist for follower: " << follower->username << std::endl;
                    }
                }


            } else {
                std::cerr << "Client object is null. Cannot process message." << std::endl;
            }
        }

        return Status::OK;
    }
};


// function that sends a heartbeat to the coordinator
IReply Heartbeat(std::string clusterId, std::string serverId, std::string hostname, std::string port, bool isHeartbeat, std::string processType) {

    IReply ire;

    ClientContext context;
    ServerInfo serverinfo;
    Confirmation confirmation;

    if (isHeartbeat){
        context.AddMetadata("heartbeat", "Hello");
    }

    context.AddMetadata("clusterid", clusterId);

    int intServerId = std::stoi(serverId);

    serverinfo.set_serverid(intServerId);
    serverinfo.set_hostname(hostname);
    serverinfo.set_port(port);
    serverinfo.set_type(processType);

    grpc::Status status = coordinator_stub_->Heartbeat(&context, serverinfo, &confirmation);
    if (status.ok()){
        ire.grpc_status = status;
    }else {
        ire.grpc_status = status;
        std::cout << "coordinator not found! exiting now...\n";
    }

    return ire;
}

// function that runs inside a detached thread that calls the heartbeat function
void sendHeartbeat(std::string clusterId, std::string serverId, std::string hostname, std::string port, std::string processType) {
    while (true){
        sleep(10);

        IReply reply = Heartbeat(clusterId, serverId, "localhost", port, true, processType);
        if (!reply.grpc_status.ok()){
            exit(1);
        }
    }
}

void InitializeSlaveStub(const std::string& slave_address) {
    slave_stub_ = MasterSlaveService::NewStub(
        grpc::CreateChannel(slave_address, grpc::InsecureChannelCredentials()));
}


void RunServer(std::string clusterId_, std::string serverId_, std::string coordinatorIP, std::string coordinatorPort, std::string port_no) {
    clusterId = clusterId_;
    serverId = serverId_;
    std::string server_address = "0.0.0.0:"+port_no;

    isMaster = (serverId == "1");
    std::string processType = isMaster ? "master" : "slave";

    // if this is a master and it dies, then the failover process starts
    if (isMaster) {
        SNSServiceImpl service;

        std::string slave_port = "10001";
        std::string slave_address = "localhost:" + slave_port;
        InitializeSlaveStub(slave_address);

        std::string coordinator_address = coordinatorIP + ":" + coordinatorPort;
        coordinator_stub_ = CoordService::NewStub(grpc::CreateChannel(coordinator_address, grpc::InsecureChannelCredentials()));

        IReply reply = Heartbeat(clusterId, serverId, "localhost", port_no, false, processType);
        if (!reply.grpc_status.ok()){
            exit(0);
        }

        std::thread myhb(sendHeartbeat, clusterId, serverId, "localhost", port_no, processType);
        myhb.detach();

        // this thread is constantly listening ot the hello_queue which is updated from 
        // synchronizers
        std::thread hbb(initializeRabbitMQAndContinuouslyConsume);
        hbb.detach();

        std::thread hb(checkHeartbeat);
        hb.detach();

        ServerBuilder builder;
        builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
        builder.RegisterService(&service);
        std::unique_ptr<Server> server(builder.BuildAndStart());
        std::cout << "Master Server listening on " << server_address << std::endl;

        server->Wait();

    } else {
        SlaveServiceImpl slave_service;

        std::string coordinator_address = coordinatorIP + ":" + coordinatorPort;
        coordinator_stub_ = CoordService::NewStub(grpc::CreateChannel(coordinator_address, grpc::InsecureChannelCredentials()));

        IReply reply = Heartbeat(clusterId, serverId, "localhost", port_no, false, processType);
        if (!reply.grpc_status.ok()){
            exit(0);
        }

        std::thread myhb(sendHeartbeat, clusterId, serverId, "localhost", port_no, processType);
        myhb.detach();

        std::thread hbb(initializeRabbitMQAndContinuouslyConsume);
        hbb.detach();

        std::thread hb(checkHeartbeat);
        hb.detach();

        ServerBuilder builder;
        builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
        builder.RegisterService(&slave_service);
        std::unique_ptr<Server> server(builder.BuildAndStart());
        std::cout << "Slave Server listening on " << server_address << std::endl;

        server->Wait();
    }
}


void checkHeartbeat(){
    while(true){
        for (const auto& pair : client_db){
            if(difftime(getTimeNow(),pair.second->last_heartbeat) > 3){
                std::cout << "missed heartbeat from client with id " << pair.first << std::endl;
                if(!pair.second->missed_heartbeat){
                    Client* current = getClient(pair.first);
                    if (current != NULL){
                        std::cout << "setting the client's values in the DB to show that it is down!\n";
                        current->connected = false;
                        current->stream = nullptr;
                        current->missed_heartbeat = true;
                        current->last_heartbeat = getTimeNow();
                    } else{
                        std::cout << "SUDDENLY, THE CLIENT CANNOT BE FOUND?!\n";
                    }
                }
            }
        }

        sleep(3);
    }
}

int main(int argc, char** argv) {

    std::string clusterId = "1";
    std::string serverId = "1";
    std::string coordinatorIP = "localhost";
    std::string coordinatorPort = "9090";
    std::string port = "1000";

    int opt = 0;
    while ((opt = getopt(argc, argv, "c:s:h:k:p:")) != -1){
        switch(opt) {
            case 'c':
                clusterId = optarg;break;
            case 's':
                serverId = optarg;break;
            case 'h':
                coordinatorIP = optarg;break;
            case 'k':
                coordinatorPort = optarg;break;
            case 'p':
                port = optarg;break;
            default:
                std::cout << "Invalid Command Line Argument\n";
        }
    }

    std::string log_file_name = std::string("server-") + port;
    google::InitGoogleLogging(log_file_name.c_str());
    log(INFO, "Logging Initialized. Server starting...");

    RunServer(clusterId, serverId, coordinatorIP, coordinatorPort, port);

    return 0;
}

std::time_t getTimeNow(){
    return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
}