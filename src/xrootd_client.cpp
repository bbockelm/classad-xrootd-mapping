
#include "xrootd_client.h"
#include "pthread_utils.h"
#include "response_cache.h"

#include <boost/tokenizer.hpp>

#include "XrdClient/XrdClientConn.hh"
#include "XrdClient/XrdClientEnv.hh"
#include "XrdClient/XrdClientDebug.hh"
#include "XrdSys/XrdSysDNS.hh"

using namespace classad;
using namespace ClassadXrootdMapping;

InstanceTable FileMappingClient::m_instance_table;
pthread_mutex_t FileMappingClient::m_table_mutex = PTHREAD_MUTEX_INITIALIZER;

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

bool FileMappingClient::map(const std::vector<std::string> &filenames, std::set<std::string> &hosts) {

	ResponseCache& cache = ResponseCache::getInstance();

	for (std::vector<std::string>::const_iterator it = filenames.begin(); it != filenames.end(); ++it)
	{
		std::set<std::string> temp_set;
		locate(*it, temp_set);
		cache.insert(*it, temp_set);
		hosts.insert(temp_set.begin(), temp_set.end());
	}

	return true;
}

FileMappingClient::FileMappingClient(const std::string &hostname)
	: m_url(("root://" + hostname).c_str()),
	m_host(hostname)
{
	m_url.SetAddrFromHost(); // No clue why the URL's constructor doesn't do this; useless without it.
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
			locallogid = m_connection.Connect(m_url, this);
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

			m_connection.SetUrl(m_url);

			Info(XrdClientDebug::kHIDEBUG, "Connect", "Working url is " << m_url.GetUrl());

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

	m_connection.SetMaxRedirCnt(1);

	return true;
}

/*
 * Note - not quite non-blocking.
 */
bool
FileMappingClient::locate_request(ClientRequest & locateRequest)
{

	// TODO: return immediately if paused.
	return m_connection.WriteToServer_Async(&locateRequest, NULL) == kOK;
	
}

bool
FileMappingClient::locate(const std::string &path, std::set<std::string> &hosts) {

	// Set the max transaction duration
	m_connection.SetOpTimeLimit(EnvGetLong(NAME_TRANSACTIONTIMEOUT));

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
		// No results!
		return true;
	} 
	//locate_request(locateRequest);

	typedef boost::tokenizer<boost::char_separator<char> > tokenizer;
	boost::char_separator<char> sep(" ");
	tokenizer tok(response_string, sep);
	Info(XrdClientDebug::kHIDEBUG, "locate", "Locate response: " << response_string);
	for(tokenizer::const_iterator single_entry=tok.begin(); single_entry!=tok.end(); ++single_entry) {

		Info(XrdClientDebug::kHIDEBUG, "locate", "Parsing single entry: " << *single_entry);
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
				Info(XrdClientDebug::kUSERDEBUG, "locate", "Invalid node type: " << (*single_entry)[0]);
				continue;
		}

		// Transform the response string to an endpoint.
		// If an IPv4 address, we cannot treat it as an opaque string.
		std::string single_entry_copy(*single_entry);
		size_t ipv4_token = single_entry_copy.find("[::", 2);
		if (ipv4_token != std::string::npos) { // IPv4
			single_entry_copy.erase(0, ipv4_token+3);
			single_entry_copy.erase(single_entry_copy.find("]"), 1);
		}
		// The above will not be necessary when Host2Dest supports IPv6.

		// This union trick is to make sure we get enough memory for the sockaddr and that
		// things are exception-safe.
		std::vector<char> sock_mem;
		sock_mem.reserve(max(sizeof(struct sockaddr_in), sizeof(struct sockaddr_in6)));
		typedef union {struct sockaddr* addr; char * memory;} sockaddr_big;
		sockaddr_big addr;
		addr.memory = &sock_mem[0];
		if (!XrdSysDNS::Host2Dest(single_entry_copy.c_str(), *(addr.addr), 0))
		{
			Error("locate", "Invalid IP address: " << single_entry_copy);
			continue;
		}

		char * hostname_result = XrdSysDNS::getHostName(*(addr.addr), NULL);
		std::string hostname(hostname_result);
		free(hostname_result);

		hosts.insert(hostname);
		Info(XrdClientDebug::kHIDEBUG, "locate", "Resulting host: " << *single_entry);
	}
	return true;
}

UnsolRespProcResult
FileMappingClient::ProcessUnsolicitedMsg(XrdClientUnsolMsgSender * /*sender*/, XrdClientMessage * unsolmsg)
{

	if (unsolmsg->IsAttn())
	{
		struct ServerResponseBody_Attn *attnbody;
		attnbody = (struct ServerResponseBody_Attn *)unsolmsg->GetData();
		int actnum = (attnbody) ? (attnbody->actnum) : 0;

		switch (actnum) {

			case kXR_asyncdi: // disconnect + delay reconnect
			case kXR_asyncrd: // redirect
				// We ignore these for now, as they break our model.
				return kUNSOL_CONTINUE;
				break;

			case kXR_asyncwt: // Put the client in wait.
				// Not so useful for us, as we'll ignore it to avoid blocking the thread.
				// TODO: record this internally instead and respect it.
				struct ServerResponseBody_Attn_asyncwt *wait;
				wait = (struct ServerResponseBody_Attn_asyncwt *)unsolmsg->GetData();
				if (wait)
				{
					m_connection.SetREQPauseState(ntohl(wait->wsec));
				}
				break;

			case kXR_asyncgo: // 
				m_connection.SetREQPauseState(0);
				return kUNSOL_CONTINUE;
				break;

			case kXR_asynresp: // An actual response.
				return kUNSOL_CONTINUE;
				break;

			default:
				break;
		}

	} // Pass communication errors to the underlying library.
	else if (unsolmsg->GetStatusCode() != XrdClientMessage::kXrdMSC_ok)
	{
		return m_connection.ProcessAsynResp(unsolmsg);
	}

	return kUNSOL_CONTINUE;
}

