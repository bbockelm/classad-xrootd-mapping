
#include "xrootd_client.h"

#include <boost/tokenizer.hpp>

#include "XrdClient/XrdClientEnv.hh"
#include "XrdClient/XrdClientDebug.hh"

using namespace classad;

InstanceTable FileMappingClient::m_instance_table;
pthread_mutex_t FileMappingClient::m_table_mutex = PTHREAD_MUTEX_INITIALIZER;

/*
 * Simple helper class for not deadlocking things.
 */
class Lock {

public:
    Lock(pthread_mutex_t & mutex) : m_mutex(mutex) {
		pthread_mutex_lock(&mutex);
    }

    ~Lock() {
        pthread_mutex_unlock(&m_mutex);
    }

private:
    // no default constructor
    Lock();

    // non-copyable.
    Lock(const Lock&);
    Lock& operator=(const Lock&);

    pthread_mutex_t & m_mutex;
};

/*
 *  Manage file mapping
 */
FileMappingClient & FileMappingClient::getClient(const std::string &hostname) {
	FileMappingClient *client = NULL;
	Lock lock(m_table_mutex);
	InstanceTable::const_iterator result = m_instance_table.find(hostname);
	if (result == m_instance_table.end()) {
		FileMappingClient *new_client = new FileMappingClient(hostname);
		m_instance_table[hostname] = new_client;
		client = new_client;
	} else {
		client = result->second;
	}
	return *client;
}

bool FileMappingClient::map(const std::vector<std::string> &filenames, std::vector<std::string> &hosts) {

	std::set<std::string> hosts_set;
	for (std::vector<std::string>::const_iterator it = filenames.begin(); it != filenames.end(); ++it)
	{
		locate(*it, hosts_set);
	}

	hosts.clear();
	hosts.reserve(hosts_set.size());
	for (std::set<std::string>::const_iterator it = hosts_set.begin(); it != hosts_set.end(); ++it)
	{
		hosts.push_back(*it);
	}
	return true;
}

FileMappingClient::FileMappingClient(const std::string &hostname)
	: m_url("root://" + hostname),
	m_host(hostname)
{
	connect();
}


//_____________________________________________________________________________
bool FileMappingClient::connect()
{              

	// Nothing to do if already connected
	if (m_connection.IsConnected()) {
		return true;
	}        

	short locallogid;

	int connectMaxTry = EnvGetLong(NAME_FIRSTCONNECTMAXCNT);

	// Set the max transaction duration
	m_connection.SetOpTimeLimit(EnvGetLong(NAME_TRANSACTIONTIMEOUT));

	//
	// Now start the connection phase, picking randomly from UrlArray
	//     
	locallogid = -1;
	for (int connectTry = 0; (connectTry < connectMaxTry) && (!m_connection.IsConnected()); connectTry++) {

		if ( m_connection.IsOpTimeLimitElapsed(time(0)) ) {
			// We have been so unlucky and wasted too much time in connecting and being redirected
			m_connection.Disconnect(TRUE);
			Error("Connect", "Access to server failed: Too much time elapsed without success.");
			break;
		}     

		if (m_connection.CheckHostDomain(m_host.c_str())) {
			Info(XrdClientDebug::kHIDEBUG, "Connect", "Trying to connect to " << m_host 
				<< ". Connect try " << connectTry+1);
			locallogid = m_connection.Connect(m_url.c_str(), this);
			// To find out if we have tried the whole URLs set
		} else {
			break; // If we don't accept this domain, bounce.
		}

		// We are connected to a host. Let's handshake with it.
		if (m_connection.IsConnected()) {
     
			// Now the have the logical Connection ID, that we can use as streamid for 
			// communications with the server

			Info(XrdClientDebug::kHIDEBUG, "Connect",
				"The logical connection id is " << m_connection.GetLogConnID() <<
				". This will be the streamid for this client");

			m_connection.SetUrl(m_url.c_str());

			Info(XrdClientDebug::kHIDEBUG, "Connect", "Working url is " << m_url);

			// after connection deal with server
			if (!m_connection.GetAccessToSrv()) {
				if (m_connection.LastServerError.errnum == kXR_NotAuthorized) {
					// Authentication error: we tried all the indicated URLs:
					// does not make much sense to retry
					m_connection.Disconnect(TRUE);
					XrdOucString msg(m_connection.LastServerError.errmsg);
					msg.erasefromend(1);
					Error("Connect", "Authentication failure: " << msg);
					connectTry = connectMaxTry;
				} else {
					Error("Connect", "Access to server failed: error: " <<
						m_connection.LastServerError.errnum << " (" << 
						m_connection.LastServerError.errmsg << ") - retrying.");
				}
			} else {
				Info(XrdClientDebug::kUSERDEBUG, "Connect", "Access to server granted.");
				break;
			}
		}

		// The server denied access. We have to disconnect.
		Info(XrdClientDebug::kHIDEBUG, "Connect", "Disconnecting.");

		m_connection.Disconnect(FALSE);

		if (connectTry < connectMaxTry-1) {

			if (DebugLevel() >= XrdClientDebug::kUSERDEBUG)
				Info(XrdClientDebug::kUSERDEBUG, "Connect", 
					"Connection attempt failed. Sleeping " <<
					EnvGetLong(NAME_RECONNECTWAIT) << " seconds.");

			sleep(EnvGetLong(NAME_RECONNECTWAIT));
		}
   
	} //for connect try

	if (!m_connection.IsConnected()) {
		return false;
	}

	// We close the connection only if we do not know the server type.
	if (m_connection.GetServerType() == kSTNone) {
		m_connection.Disconnect(TRUE);
		return false;
	}

	return true;
}

bool
FileMappingClient::locate(const std::string &path, std::set<std::string> &hosts) {

	ClientRequest locateRequest;
	memset(&locateRequest, 0, sizeof(locateRequest));

	m_connection.SetSID(locateRequest.header.streamid);
   
	locateRequest.locate.requestid = kXR_locate;
	locateRequest.locate.options   = kXR_nowait;
	locateRequest.locate.dlen	   = path.size();
   
	char *resp = 0;
	if (!m_connection.SendGenCommand(&locateRequest, path.c_str(),
			(void **)&resp, 0, true, (char *)"LocalLocate")) {
		return false;
	}
	if (!resp) return false;
	std::string response_string(resp);
	free(resp);
	if (!response_string.size()) {
		return -1;
	} 

	boost::tokenizer<> tok(response_string);
	for(boost::tokenizer<>::const_iterator single_entry=tok.begin(); single_entry!=tok.end(); ++single_entry) {

		// Current format (from http://xrootd.org/doc/prod/XRdv298.htm#_Toc235365554):
		// xy[::aaa.bbb.ccc.ddd.eee]:ppppp
		// x = node type
		// y = r (read) or w (write)
		// [::aaa.bbb.ccc.ddd.eee] = if IPv6, the entire IP address; if IPv4, aaa.bbb.ccc.ddd.eee is the IP address
		// ppppp = port
		if (single_entry->size() < 8 || ((*single_entry)[2] != '[') || ((*single_entry)[4] != ':')) {
			continue;
		}

		// Info type
		switch ((*single_entry)[0]) {
			case 'S': // Data server
			case 's': // Pending data server
			case 'M': // Manager
			case 'm': // Pending manager
				break;

			default: // Invalid response
				continue;
		}

		// Transform the response string to an endpoint.
		// If an IPv4 address, we cannot treat it as an opaque string.
		std::string single_entry_copy(*single_entry);
		size_t ipv4_token = single_entry_copy.find("[::", 2);
		if (ipv4_token != std::string::npos) { // IPv4
			single_entry_copy.erase(0, ipv4_token+3);
			single_entry_copy.erase(single_entry_copy.find("]"));
		}
		hosts.insert(single_entry_copy);
	}

	return true;
}

