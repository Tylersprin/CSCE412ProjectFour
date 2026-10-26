/**
 * @file LoadBalancer.h
 * @brief Defines the LoadBalancer class for managing server threads and request queues.
 */

#include <queue>
#include <map>
#include "Server.h"
#include <mutex>
#include <atomic>
using namespace std;

/**
 * @class LoadBalancer
 * @brief Dynamically scales and manages server threads based on request queue load.
 */
class LoadBalancer {
    private:
        atomic<bool> shuttingDown = false;    ///< Flag indicating a server is shutting down.
        atomic<bool> kill = false;            ///< Flag to terminate the load balancer loop.
        atomic<bool> done = false;            ///< Flag indicating termination sequence completion.
        atomic<int>& cycles;                  ///< Reference to global cycle count.
        atomic<int>& quiet;                   ///< Reference to quiet logging counter.
        atomic<int> numServers = 0;           ///< Count of active servers.
        atomic<int> numProcessed = 0;         ///< Count of successfully processed requests.
        map<Server*, bool> living;            ///< Tracks living servers.
        map<Server*, bool> dying;             ///< Tracks servers scheduled for termination.
        map<Server*, thread> threads;         ///< Maps servers to their corresponding threads.
        queue<Request> requestQueue;          ///< Incoming request queue.
        int maxSize;                          ///< Maximum capacity of the request queue.
        int delay;                            ///< Delay configuration parameter.
        mutable std::mutex mtx;               ///< Internal mutex for thread synchronization.
        mutex& logmtx;                        ///< Reference to global log mutex.
        ofstream& verboseLog;                 ///< Reference to verbose log stream.
        ofstream& quietLog;                   ///< Reference to quiet log stream.
        thread t;                             ///< Main balancer thread.
        string type;                          ///< Request type handled by this load balancer.

        /**
         * @brief Core worker routine executed by each server thread.
         * @param serv Pointer to the Server instance.
         */
        void runServer(Server* serv) {
            while (true) {
                Request next;
                bool proc = false;
                {
                    std::lock_guard<std::mutex> lock(mtx);
                    if (!requestQueue.empty()) {
                        next = requestQueue.front();
                        requestQueue.pop();
                        proc = true;
                    }
                }
                if (proc) {
                    serv->process(next);
                    numProcessed++;
                    {
                        std::lock_guard<std::mutex> lock(logmtx);
                        verboseLog << cycles.load() << ": Server processing request (IP-IN = " << next.IPIN << ", IP-OUT = " << next.IPOUT << ", type = " << next.type << ", time = " << next.time << ")" << endl;
                        if (serv->quiet) {
                            quietLog << cycles.load() << ": Server processing request (IP-IN = " << next.IPIN << ", IP-OUT = " << next.IPOUT << ", type = " << next.type << ", time = " << next.time << ")" << endl;
                        }
                    }
                }
                std::lock_guard<std::mutex> lock(mtx);
                if (!living[serv]) {
                    break;
                }
            }
            std::lock_guard<std::mutex> lock(mtx);
            dying[serv] = true;
        }

        /**
         * @brief Main monitoring loop for scaling servers up or down based on load.
         */
        void runBalancer() {
            std::this_thread::sleep_for(std::chrono::milliseconds(delay + 50));
            int counter = 0;
            while (!kill) {
                bool sleep = false;
                counter++;
                if (counter % 100000 == 0) {
                    {
                        std::lock_guard<std::mutex> lock(logmtx);
                        verboseLog << endl << cycles.load() << ": Load Balancer of type " << type << " report,  " << requestQueue.size() << " requests and " << living.size() << " servers" << endl << endl;
                    }
                }
                {
                    std::lock_guard<std::mutex> lock(mtx);
                    if (requestQueue.size() > living.size() * 80) {
                        Server* serv = new Server();
                        if (quiet < 20) {
                            serv->quiet = true;
                            quiet++;
                        }
                        {
                            std::lock_guard<std::mutex> lock(logmtx);
                            verboseLog << cycles.load() << ": Load Balancer of type " << type << " overwhelmed with " << requestQueue.size() << " requests and " << living.size() << " servers, starting new server"<< endl;
                            if (serv->quiet) {
                                quietLog << cycles.load() << ": Load Balancer of type " << type << " overwhelmed with " << requestQueue.size() << " requests and " << living.size() << " servers, starting new server"<< endl;
                            }
                        }
                        living[serv] = true;
                        numServers++;
                        threads[serv] = std::thread(&LoadBalancer::runServer, this, serv);
                        sleep = true;
                    }
                    if (requestQueue.size() < living.size() * 50 && living.size() != 1 && !shuttingDown) {
                        for (const auto& [serv, alive] : living) {
                            if (alive) {
                                {
                                    std::lock_guard<std::mutex> lock(logmtx);
                                    verboseLog << cycles.load() << ": Load Balancer of type " << type << " underwhelmed with " << requestQueue.size() << " requests and " << living.size() << " servers, shutting down a server"<< endl;
                                }
                                shuttingDown = true;
                                living[serv] = false;
                                sleep = true;
                                break;
                            }
                        }
                    }
                    for (const auto& [serv, dead] : dying) {
                        if (dead) {
                            living.erase(serv);
                            dying.erase(serv);
                            threads[serv].join();
                            threads.erase(serv);
                            shuttingDown = false;
                            {
                                std::lock_guard<std::mutex> lock(logmtx);
                                verboseLog << cycles.load() << ": Load Balancer of type " << type << " server shutdown complete" << endl;
                                if (serv->quiet) {
                                    quietLog << cycles.load() << ": Load Balancer of type " << type << " server shutdown complete" << endl;
                                }

                            }
                            delete serv;
                            break;
                        }
                    }
                }
                if (sleep) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }
            }
            for (auto& [serv, thread] : threads) {
                {
                    std::lock_guard<std::mutex> lock(mtx);
                    living[serv] = false;
                }
            }
            for (auto& [serv, thread] : threads) {
                thread.join();
                {
                    std::lock_guard<std::mutex> lock(logmtx);
                    verboseLog << cycles.load() << ": Load Balancer of type " << type << " server shutdown complete" << endl;
                    if (serv->quiet) {
                        quietLog << cycles.load() << ": Load Balancer of type " << type << " server shutdown complete" << endl;
                    }
                }
                delete serv;
            }
            threads.clear();
            done = true;
        }

    public:
        /**
         * @brief Construct a new Load Balancer object.
         * 
         * @param size Maximum queue size limit.
         * @param servers Initial number of servers to spin up.
         * @param del Delay configuration value.
         * @param mtx Logging mutex reference.
         * @param verb Verbose log stream reference.
         * @param quietL Quiet log stream reference.
         * @param cycle Cycles counter reference.
         * @param q Quiet log counter reference.
         * @param typ String label for the load balancer type.
         */
        LoadBalancer(int size, int servers, int del, mutex& mtx, ofstream& verb, ofstream& quietL, atomic<int>& cycle, atomic<int>& q, string typ) : logmtx(mtx), verboseLog(verb), quietLog(quietL), cycles(cycle), quiet(q) {
            maxSize = size;
            type = typ;
            delay = del;
            for (int i = 0; i < servers; i++) {
                Server* serv = new Server();
                if (quiet < 20) {
                    quiet++;
                    serv->quiet = true;
                }
                living[serv] = true;
                numServers++;
                threads[serv] = std::thread(&LoadBalancer::runServer, this, serv);
                {
                    std::lock_guard<std::mutex> lock(logmtx);
                    verboseLog << cycles.load() << ": Load Balancer of type " << type << " starting server" << endl;
                    if (serv->quiet) {
                        quietLog << cycles.load() << ": Load Balancer of type " << type << " starting server" << endl;
                    }
                }
            }
            {
                std::lock_guard<std::mutex> lock(logmtx);
                verboseLog << cycles.load() << ": Load Balancer of type " << type << " initialized" << endl;
            }
            t = thread(&LoadBalancer::runBalancer, this);
        }

        /**
         * @brief Destroy the Load Balancer object and gracefully join all active threads.
         */
        ~LoadBalancer() {
            {
                std::lock_guard<std::mutex> lock(logmtx);
                verboseLog << cycles.load() << ": Load Balancer of type " << type << " shutting down" << endl;
            }
            kill = true;
            while (!done) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            t.join();
            {
                std::lock_guard<std::mutex> lock(logmtx);
                verboseLog << cycles.load() << ": Load Balancer of type " << type << " shutdown complete" << endl;
            }
        }

        /**
         * @brief Safely retrieves the current request queue size.
         * @return int Size of the queue.
         */
        int getQueueSize() {
            std::lock_guard<std::mutex> lock(mtx);
            return requestQueue.size();
        }

        /**
         * @brief Retrieves the number of servers spawned.
         * @return int Server count.
         */
        int getServers() {
            return numServers.load();
        }

        /**
         * @brief Retrieves the total number of processed requests.
         * @return int Processed count.
         */
        int getProcessed() {
            return numProcessed.load();
        }

        /**
         * @brief Enqueues a new request if capacity allows.
         * @param r The Request object to push.
         */
        void push(Request r) {
            std::lock_guard<std::mutex> lock(mtx);
            if (requestQueue.size() < maxSize) {
                requestQueue.push(r);
            }
            {
                std::lock_guard<std::mutex> lock(logmtx);
                verboseLog << cycles.load() << ": Load Balancer of type " << type << " request recieved" << endl;
            }
        }
};