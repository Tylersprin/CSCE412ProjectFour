/**
 * @file Server.h
 * @brief Defines the Server class that simulates processing individual network requests.
 */

#include <thread>
#include <chrono>
#include "Request.h"

/**
 * @class Server
 * @brief Represents a single processing unit that handles requests over a given duration.
 */
class Server {
    public:
        bool quiet = false; ///< Flag indicating if server logs should also go to the quiet log.

        /**
         * @brief Construct a new Server object.
         */
        Server() {}

        /**
         * @brief Simulates processing a request by sleeping for the specified request duration.
         * @param r The Request object being processed.
         */
        void process(Request r) {
            std::this_thread::sleep_for(std::chrono::milliseconds(r.time));
        }
};