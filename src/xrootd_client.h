
#include <set>
#include <string>
#include <vector>

#include "classad/classad_distribution.h"

#include "XrdClient/XrdClientUnsolMsg.hh"
#include "XrdClient/XrdClientConn.hh"

class FileMappingClient;

typedef classad_hash_map<std::string, FileMappingClient*, classad::StringHash> InstanceTable;

/*
 *  Manage file mapping
 */
class FileMappingClient : public XrdClientAbsUnsolMsgHandler {

public:
	static FileMappingClient &getClient(const std::string &hostname);

	bool map(const std::vector<std::string> &, std::vector<std::string> &);

	bool is_connected() {return m_connection.IsConnected();}

	UnsolRespProcResult ProcessUnsolicitedMsg(XrdClientUnsolMsgSender * /*sender*/, XrdClientMessage * /*unsolmsg*/) {return kUNSOL_CONTINUE;};

private:
	FileMappingClient(const std::string &hostname);

	bool connect();
	bool locate(const std::string &, std::set<std::string> &);

	XrdClientConn m_connection;
	XrdClientUrlInfo m_url;
	std::string m_host;

	static InstanceTable m_instance_table;
	static pthread_mutex_t m_table_mutex;
};

