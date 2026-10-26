/**
 * @file Switch.h
 * @brief Defines the Switch class which acts as a router/firewall for incoming requests.
 */

#include <vector>
#include <queue>
#include "LoadBalancer.h"
using namespace std;

/**
 * @class Switch
 * @brief Manages routing requests to appropriate type-specific Load Balancers and enforces firewall rules.
 */
class Switch {
    private:
        mutex& logmtx;                      ///< Reference to global logging mutex.
        map<string, LoadBalancer*> Balancers; ///< Collection of type-mapped Load Balancers.
        map<string, bool> types;            ///< Tracks available request types.
        ofstream& verboseLog;               ///< Reference to verbose log file stream.
        int maxSize;                        ///< Maximum request queue size for load balancers.
        atomic<int>& cycles;                ///< Reference to global cycle counter.
        atomic<int> quiet = 0;              ///< Counter for quiet-logged elements.
        ofstream& quietLog;                 ///< Reference to quiet log file stream.

    public:
        /**
         * @brief Construct a new Switch object.
         * 
         * @param size Maximum queue size.
         * @param delay Initial thread delay configuration.
         * @param balance Vector mapping server counts to request types.
         * @param mtx Logging mutex reference.
         * @param verb Verbose log stream reference.
         * @param quietL Quiet log stream reference.
         * @param cycle Global cycles counter reference.
         */
        Switch(int size, int delay, vector<pair<int, string>> balance, mutex& mtx, ofstream& verb, ofstream& quietL, atomic<int>& cycle) : logmtx(mtx), verboseLog(verb), quietLog(quietL), cycles(cycle) {
            maxSize = size;
            for (const auto& [num, type] : balance) {
                {
                    std::lock_guard<std::mutex> lock(logmtx);
                    verboseLog << cycles.load() << ": Initializing load balancer of type " << type << endl;
                }
                LoadBalancer* next = new LoadBalancer(maxSize, num, delay, logmtx, verboseLog, quietLog, cycles, quiet, type);
                Balancers[type] = next;
                types[type] = true;
            }
        }

        /**
         * @brief Pushes a request through the firewall and directs it to the correct load balancer.
         * 
         * @param r The Request object to process.
         */
        void push(Request r) {
            if (stoi(r.IPIN.substr(0, 3)) < 50 || stoi(r.IPOUT.substr(0, 3)) > 200) {
                {
                    std::lock_guard<std::mutex> lock(logmtx);
                    verboseLog << cycles.load() << ": Firewall blocked request (IP-IN = " << r.IPIN << ", IP-OUT = " << r.IPOUT << ", type = " << r.type << ", time = " << r.time << ")" << endl;
                    return;
                }
            }
            if (!types[r.type]) {
                {
                    std::lock_guard<std::mutex> lock(logmtx);
                    verboseLog << cycles.load() << ": Initializing load balancer of type " << r.type << endl;
                }
                LoadBalancer* next = new LoadBalancer(maxSize, 0, 0, logmtx, verboseLog, quietLog, cycles, quiet, r.type);
                types[r.type] = true;
            }
            {
                std::lock_guard<std::mutex> lock(logmtx);
                verboseLog << cycles.load() << ": Switch received request of type " << r.type << " sending to load balancer" << endl;
            }
            Balancers[r.type]->push(r);
        }

        /**
         * @brief Gets the total queue size across all active load balancers.
         * @return int Combined queue size.
         */
        int size() {
            int size = 0;
            for (const auto& [idc, balancer] : Balancers) {
                size += balancer->getQueueSize();
            }
            return size;
        }

        /**
         * @brief Gets the total count of managed load balancers.
         * @return int Number of load balancers.
         */
        int getLoadbalancers() {
            return Balancers.size();
        }

        /**
         * @brief Gets the total count of active servers across all load balancers.
         * @return int Total server count.
         */
        int getServers() {
            int size = 0;
            for (const auto& [idc, balancer] : Balancers) {
                size += balancer->getServers();
            }
            return size;
        }

        /**
         * @brief Gets the total count of requests processed across all load balancers.
         * @return int Total processed requests.
         */
        int getRequests() {
            int size = 0;
            for (const auto& [idc, balancer] : Balancers) {
                size += balancer->getProcessed();
            }
            return size;
        }

        /**
         * @brief Destroy the Switch object and clean up all underlying load balancers.
         */
        ~Switch() {
            for (const auto& [idc, balancer] : Balancers) {
                delete balancer;
            }
        }
};