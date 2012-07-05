
#include <set>
#include <string>
#include <vector>

#include "classad/classad_distribution.h"

#include "XrdClient/XrdClientUnsolMsg.hh"
#include "XrdClient/XrdClientConn.hh"

namespace ClassadXrootdMapping {

class FileMappingClient;

typedef classad_unordered<std::string, FileMappingClient*> InstanceTable;

/*
 *  Manage file mapping
 */
class FileMappingClient : public XrdClientAbsUnsolMsgHandler {

public:
	static FileMappingClient &getClient(const std::string &hostname);

	bool map(const std::vector<std::string> & filenames, std::set<std::string> & output_hosts);

	bool is_connected() {return m_connection.IsConnected();}

	UnsolRespProcResult ProcessUnsolicitedMsg(XrdClientUnsolMsgSender * /*sender*/, XrdClientMessage * /*unsolmsg*/);

private:
	FileMappingClient(const std::string &hostname);

	bool connect();
	bool locate(const std::string &, std::set<std::string> &);
	bool locate_request(ClientRequest&);

	XrdClientConn m_connection;
	XrdClientUrlInfo m_url;
	std::string m_host;

	static InstanceTable m_instance_table;
	static pthread_mutex_t m_table_mutex;
};

}
