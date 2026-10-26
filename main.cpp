/**
 * @file main.cpp
 * @brief Main entry point for the Load Balancer simulation project.
 * 
 * Handles command-line arguments, initializes logging systems, generates 
 * randomized network requests, and manages the simulation lifecycle across 
 * various time cycles.
 */

#include "Switch.h"
#include <queue>
#include <map>
#include <string>
#include <filesystem>
using namespace std;

/**
 * @brief Generates a randomized network request.
 * 
 * Creates a request with a random type ("Streaming" or "Processing"), 
 * processing duration between 1-200 ms, and randomized source/destination IP addresses.
 * 
 * @return Request The generated Request object.
 */
Request generate() {
    vector<string> types = {"Streaming", "Processing"};
    int type = (rand() % 2);
    int time = (rand() % 200) + 1;
    string IPIN = to_string((rand() % 256) + 1) + "." + to_string((rand() % 256) + 1) + "." + to_string((rand() % 256) + 1) + "." + to_string((rand() % 256) + 1);
    string IPOUT = to_string((rand() % 256) + 1) + "." + to_string((rand() % 256) + 1) + "." + to_string((rand() % 256) + 1) + "." + to_string((rand() % 256) + 1);
    Request ret = Request(IPIN, IPOUT, time, types[type]);
    return ret;
}

/**
 * @brief Main simulation runner.
 * 
 * Parses command-line inputs, sets up verbose and quiet log files, seeds initial 
 * requests, creates the central Switch, and steps through the simulation cycles.
 * 
 * @param argc Argument count (expects 4 parameters + program name).
 * @param argv Argument vector: [1] servers, [2] delay, [3] maxSize, [4] verbose flag.
 * @return int Exit status code (0 on success, -1 on invalid arguments).
 */
int main(int argc, char* argv[]) {
    if (argc != 5) {
        return -1;
    }
    mutex logmtx;
    srand(time(NULL));
    ofstream verboseLog("verboseLog.txt");
    ofstream quietLog("quietLog.txt");
    int servers = stoi(argv[1]);
    int delay = stoi(argv[2]);
    int maxSize = stoi(argv[3]);
    int verbose = stoi(argv[4]);
    queue<Request> q;
    map<string, double> count;
    vector<pair<int, string>> balance;
    verboseLog << "Beginning Process:" << endl << "Servers: " << servers << endl << "Delay: " << delay << endl << "Max Queue Size: " << maxSize << endl << endl << endl;
    verboseLog << "Generating initial requests (" << servers * 100 << " requests)" << endl;
    quietLog << "Task time range: 1-200 ms" << endl;
    quietLog << "Starting queue size: " << servers * 100 << endl << endl << endl << endl;

    for (int i = 0; i < servers * 100; i++) {
        Request next = generate();
        count[next.type]++;
        q.push(next);
    }
    for (const auto& [type, counts] : count) {
        balance.push_back({static_cast<int>(counts / 100.0 + 1), type});
    }
    verboseLog << "Initializing Switch" << endl;
    verboseLog << "waiting..." << endl;
    atomic<int> cycles = 0;
    Switch* sw = new Switch(maxSize, delay, balance, logmtx, verboseLog, quietLog, cycles);
    std::this_thread::sleep_for(std::chrono::milliseconds(delay));
    while (!q.empty()) {
        sw->push(q.front());
        q.pop();
    }
    {
        std::lock_guard<std::mutex> lock(logmtx);
        verboseLog << "Beginning process cycles" << endl;
    }
    while (cycles < 30000) {
        int random = (rand() % 7) + 1;
        {
            std::lock_guard<std::mutex> lock(logmtx);
            verboseLog << cycles << ": Recieved " << random << " requests" << endl;
        }
        for (int i = 0; i < random; i++) {
            sw->push(generate());
        }
        random = (rand() % 5) + 1;
        std::this_thread::sleep_for(std::chrono::milliseconds(random));
        cycles += random;
    }
    while (cycles < 70000) {
        int random = (rand() % 12) + 1;
        {
            std::lock_guard<std::mutex> lock(logmtx);
            verboseLog << cycles << ": Recieved " << random << " requests" << endl;
        }
        for (int i = 0; i < random; i++) {
            sw->push(generate());
        }
        random = (rand() % 5) + 1;
        std::this_thread::sleep_for(std::chrono::milliseconds(random));
        cycles += random;
    }
    while (cycles < 100000) {
        int random = (rand() % 3) + 1;
        {
            std::lock_guard<std::mutex> lock(logmtx);
            verboseLog << cycles << ": Recieved " << random << " requests" << endl;
        }
        for (int i = 0; i < random; i++) {
            sw->push(generate());
        }
        random = (rand() % 5) + 1;
        if (cycles + random > 100000) {
            random = 100000 - cycles;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(random));
        cycles += random;
    }
    {
        std::lock_guard<std::mutex> lock(logmtx);
        verboseLog << "Process complete, shutting down" << endl;
        quietLog << endl << endl << endl << "Ending queue size: " << sw->size() << endl;
        quietLog << "Load Balancers created: " << sw->getLoadbalancers() << endl;
        quietLog << "Servers created: " << sw->getServers() << endl;
        quietLog << "Requests processed: " << sw->getRequests() << endl;
    }
    delete sw;
    {
        std::lock_guard<std::mutex> lock(logmtx);
        verboseLog << "Shutdown complete" << endl;
    }
    verboseLog.close();
    quietLog.close();
    if (verbose == 0) {
        filesystem::remove("verboseLog.txt");
    }
}