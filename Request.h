/**
 * @file Request.h
 * @brief Defines the Request class encapsulating network packet properties.
 */

#include <iostream>
#include <fstream>
using namespace std;

/**
 * @class Request
 * @brief Represents a single network request item containing source/destination IPs, duration, and type.
 */
class Request {
    public:
        string IPIN;   ///< Source IP address.
        string IPOUT;  ///< Destination IP address.
        int time;      ///< Processing time duration in milliseconds.
        string type;   ///< Request classification type (e.g., "Streaming", "Processing").

        /**
         * @brief Construct a new Request object with parameters.
         * 
         * @param IPI Source IP string.
         * @param IPO Destination IP string.
         * @param tim Task execution duration.
         * @param typ Task type classification.
         */
        Request(string IPI, string IPO, int tim, string typ) {
            IPIN = IPI;
            IPOUT = IPO;
            time = tim;
            type = typ;
        }

        /**
         * @brief Construct a new default Request object.
         */
        Request() {}
};