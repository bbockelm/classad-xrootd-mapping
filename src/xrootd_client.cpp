
#include <netinet/in.h>
#include "xrootd_client.h"
#include "response_cache.h"

#include "XrdCl/XrdClFileSystem.hh"
#include "XrdSys/XrdSysDNS.hh"

using namespace classad;
using namespace ClassadXrootdMapping;
using namespace XrdCl;

InstanceTable FileMappingClient::m_instance_table;
XrdSysMutex FileMappingClient::m_table_mutex;

/*
 *  Manage file mapping
 */
FileMappingClient & FileMappingClient::getClient(const std::string &hostname) {
	FileMappingClient *client = NULL;
	XrdSysMutexHelper lock(m_table_mutex);
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
	: m_url("root://" + hostname),
	m_host(hostname),
	m_fs(m_url)
{
}

bool
FileMappingClient::locate(const std::string &path, std::set<std::string> &hosts) {

	// I hate the pointer ownership here...
    FileMappingResponseHandler *handler_ptr = new FileMappingResponseHandler();

	XRootDStatus status = m_fs.Locate(path, OpenFlags::NoWait, handler_ptr, 0);
 
	if (!status.IsOK())
	{ // TODO: log message
		return false;
	}

	if (handler_ptr->WaitForResponseMS(50))
	{
		// Timeout - handler object will report directly to cache.
		// Note that we leak the handler object - it will delete itself.
		return true;
	}
	std::auto_ptr<FileMappingResponseHandler> handler(handler_ptr);
	XRootDStatus hstatus;
    if (!handler->GetStatus(hstatus) || !hstatus.IsOK())
	{
		return false;
	}
	LocationInfo info;
	if (!handler->GetResponse(info))
	{
		return false;
	}

	for (LocationInfo::ConstIterator it = info.Begin(); it!=info.End(); it++)
	{

		it->GetAddress();

		// Transform the response string to an endpoint.
		// If an IPv4 address, we cannot treat it as an opaque string.
		std::string single_entry_copy = it->GetAddress();
		size_t ipv4_token = single_entry_copy.find("[::");
		if (ipv4_token != std::string::npos) { // IPv4
			single_entry_copy.erase(0, ipv4_token+3);
			single_entry_copy.erase(single_entry_copy.find("]"), 1);
		}
		// The above will not be necessary when Host2Dest supports IPv6.

		// This union trick is to make sure we get enough memory for the sockaddr and that
		// things are exception-safe.
		std::vector<char> sock_mem;
		sock_mem.reserve(std::max(sizeof(struct sockaddr_in), sizeof(struct sockaddr_in6)));
		typedef union {struct sockaddr* addr; char * memory;} sockaddr_big;
		sockaddr_big addr;
		addr.memory = &sock_mem[0];
		if (!XrdSysDNS::Host2Dest(single_entry_copy.c_str(), *(addr.addr), 0))
		{
			//Error("locate", "Invalid IP address: " << single_entry_copy);
			continue;
		}

		char * hostname_result = XrdSysDNS::getHostName(*(addr.addr), NULL);
		std::string hostname(hostname_result);
		free(hostname_result);

		hosts.insert(hostname);
	}
	return true;
}

void FileMappingResponseHandler::HandleResponse( XrdCl::XRootDStatus *status, XrdCl::AnyObject *response )
{
	XrdSysCondVarHelper sentry(pCond);
	pStatus = status;
	pResponse = response;
	// Do not allow re-ordering of the above assignments
	// after the below AtomicInc.  If there are no atomics,
	// everything is protected by a mutex and we can safely
	// skip the call.
#ifdef HAVE_ATOMICS
	__sync_synchronize();
#endif
	AtomicInc(pValid);
	pCond.Broadcast();

	if (pWaitedOnce)
	{
		// TODO: Register the file in the cache ourselve.
	}
}

