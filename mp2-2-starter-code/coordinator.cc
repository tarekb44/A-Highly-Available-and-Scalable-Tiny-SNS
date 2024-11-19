#include <algorithm>
#include <cstdio>
#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include <chrono>
#include <sys/stat.h>
#include <sys/types.h>
#include <utility>
#include <vector>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <mutex>
#include <stdlib.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>

#include "coordinator.grpc.pb.h"
#include "coordinator.pb.h"

using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using csce662::CoordService;
using csce662::ServerInfo;
using csce662::Confirmation;
using csce662::ID;
using csce662::ServerList;
using csce662::SynchService;


std::time_t getTimeNow(){
    return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
}

struct zNode{
    int serverID;
    std::string hostname;
    std::string port;
    std::string type; // "master" or "slave"
    std::time_t last_heartbeat;
    bool missed_heartbeat;
    bool isActive;

    bool isAlive() {
        return !missed_heartbeat || difftime(getTimeNow(), last_heartbeat) < 20;
    }
};

//potentially thread safe 
std::mutex v_mutex;
std::vector<zNode*> cluster1;
std::vector<zNode*> cluster2;
std::vector<zNode*> cluster3;

// creating a vector of vectors containing znodes
std::vector<std::vector<zNode*>> clusters = {cluster1, cluster2, cluster3};


//func declarations
int findServer(std::vector<zNode*> v, int id); 
std::time_t getTimeNow();
void checkHeartbeat();


// this function returns the index of the required server in its cluster array
int findServer(std::vector<zNode*> v, int id){
    v_mutex.lock();

    for (size_t i = 0; i < v.size(); ++i) {
        if (v[i]->serverID == id) {
            v_mutex.unlock();
            return i; // Return the index of the zNode with the matching serverId
        }
    }

    if (v.size() > 0){ // if a server with the exact specified serverId was not found, just return the very first server in the cluster instead
        v_mutex.unlock();
        return 0;
    }

    v_mutex.unlock();

    // at this point no appropriate server was found
    return -1;  
}

class CoordServiceImpl final : public CoordService::Service {

    Status Heartbeat(ServerContext* context, const ServerInfo* serverinfo, Confirmation* confirmation) override {

        const std::multimap<grpc::string_ref, grpc::string_ref>& metadata = context->client_metadata();

        std::string clusterid;
        int intClusterid;
        auto it = metadata.find("clusterid");
        if (it != metadata.end()) {
            std::string customValue(it->second.data(), it->second.length());

            clusterid = customValue;
            intClusterid = std::stoi(clusterid);
        }

        std::string processType = serverinfo->type();
        int serverID = serverinfo->serverid();

        std::cout<<"Received Heartbeat! ServerID: "<< processType <<"("<< serverID <<"), ClusterID: (" << clusterid << ")\n";

        v_mutex.lock();
        std::vector<zNode*>& cluster = clusters[intClusterid - 1];
        auto node_it = std::find_if(cluster.begin(), cluster.end(), [serverID](zNode* node) {
            return node->serverID == serverID;
        });

        if (node_it != cluster.end()) {
            zNode* node = *node_it;
            node->last_heartbeat = getTimeNow();
            node->missed_heartbeat = false;
            node->type = processType;
        } else {
            zNode* node = new zNode();
            node->serverID = serverID;
            node->hostname = serverinfo->hostname();
            node->port = serverinfo->port();
            node->type = processType;
            node->last_heartbeat = getTimeNow();
            node->missed_heartbeat = false;
            node->isActive = true;
            cluster.push_back(node);
            std::cout << "New server registered: " << processType << " in Cluster " << clusterid << std::endl;
        }
        v_mutex.unlock();

        confirmation->set_status(true);
        return Status::OK;
    }

    Status GetServer(ServerContext* context, const ID* id, ServerInfo* serverinfo) override {
        std::cout<<"Got GetServer for clientID: "<<id->id()<<std::endl;
        int clientID = id->id();
        int clusterId = ((clientID - 1) % 3) + 1;

        v_mutex.lock();
        std::vector<zNode*>& cluster = clusters[clusterId - 1];
        zNode* masterNode = nullptr;

        for (auto& node : cluster) {
            if (node->type == "master" && node->isActive && node->isAlive()) {
                masterNode = node;
                break;
            }
        }

        if (masterNode) {
            serverinfo->set_hostname(masterNode->hostname);
            serverinfo->set_port(masterNode->port);
            serverinfo->set_serverid(masterNode->serverID);
            serverinfo->set_type("master");
            v_mutex.unlock();
            return Status::OK;
        } else {
            v_mutex.unlock();
            return Status(grpc::NOT_FOUND, "No active Master found");
        }
    }

};


void RunServer(std::string port_no){
    //start thread to check heartbeats
    std::thread hb(checkHeartbeat);
    hb.detach();
    //localhost = 127.0.0.1
    std::string server_address("127.0.0.1:"+port_no);
    CoordServiceImpl service;
    //grpc::EnableDefaultHealthCheckService(true);
    //grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&service);
    // Finally assemble the server.
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;

    // Wait for the server to shutdown. Note that some other thread must be
    // responsible for shutting down the server for this call to ever return.
    server->Wait();
}

int main(int argc, char** argv) {

    std::string port = "3010";
    int opt = 0;
    while ((opt = getopt(argc, argv, "p:")) != -1){
        switch(opt) {
            case 'p':
                port = optarg;
                break;
            default:
                std::cerr << "Invalid Command Line Argument\n";
        }
    }
    RunServer(port);
    return 0;
}



void checkHeartbeat(){
    while(true){
        v_mutex.lock();

        for (size_t i = 0; i < clusters.size(); ++i){
            std::vector<zNode*>& cluster = clusters[i];
            zNode* masterNode = nullptr;
            zNode* slaveNode = nullptr;

            for (auto& node : cluster) {
                if (node->type == "master") {
                    masterNode = node;
                } else if (node->type == "slave") {
                    slaveNode = node;
                }
            }

            if (masterNode && !masterNode->isAlive()) {
                std::cout << "Master " << masterNode->serverID << " in Cluster " << (i+1) << " failed.\n";

                if (slaveNode && slaveNode->isAlive()) {
                    std::cout << "Promoting Slave " << slaveNode->serverID << " to Master.\n";
                    slaveNode->type = "master";
                    masterNode->type = "slave";
                } else {
                    std::cout << "No Slave available to promote in Cluster " << (i+1) << ".\n";
                }
            }

            for (auto& node : cluster) {
                if (difftime(getTimeNow(), node->last_heartbeat) > 20) {
                    node->missed_heartbeat = true;
                    node->isActive = false;
                }
            }
        }

        v_mutex.unlock();
        sleep(5);
    }
}



