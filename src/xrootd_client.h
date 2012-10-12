
#include <set>
#include <string>
#include <vector>

#include "classad/classad_distribution.h"

#include "XrdCl/XrdClXRootDResponses.hh"
#include "XrdCl/XrdClFileSystem.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "XrdSys/XrdSysAtomics.hh"

namespace ClassadXrootdMapping {

class FileMappingClient;
class FileMappingResponseHandler;

typedef classad_unordered<std::string, FileMappingClient*> InstanceTable;

/*
 *  Manage file mapping
 */
class FileMappingClient {

public:
	static FileMappingClient &getClient(const std::string &hostname);

	bool map(const std::vector<std::string> & filenames, std::set<std::string> & output_hosts);

private:
	FileMappingClient(const std::string &hostname);

	bool locate(const std::string &, std::set<std::string> &);

	std::string m_url;
	std::string m_host;
	XrdCl::FileSystem m_fs;

	static InstanceTable m_instance_table;
	static XrdSysMutex m_table_mutex;
};

/*
 * Asynchronous handler for the location request.
 */
class FileMappingResponseHandler : public XrdCl::ResponseHandler
{

public:

	FileMappingResponseHandler():
		pStatus(0),
		pResponse(0),
		pValid(0),
		pWaitedOnce(0),
		pCond(0)
	{}

	virtual void HandleResponse( XrdCl::XRootDStatus *status, XrdCl::AnyObject *response );

	int WaitForResponseMS(unsigned int waitTime)
	{
		XrdSysCondVarHelper sentry(pCond);
		// We don't call AtomicBeg/End here because the pCond
		// is always locked.
		int valid = AtomicGet(pValid);
		if (valid)
			return 0;
		pCond.WaitMS(waitTime);
        pWaitedOnce = 1;
		if (!valid)
			return 1;
		return 0;
	}

	inline int isValid()
	{
		int valid;
		AtomicBeg(pCond);
		valid = AtomicGet(pValid);
		AtomicEnd(pCond);
		return valid;
	}

	inline bool GetResponse(XrdCl::LocationInfo &info)
	{
		if (isValid() && pStatus->IsOK())
		{
			XrdCl::LocationInfo *linfo = 0;
			pResponse->Get(linfo);
			if (!linfo)
				return false;
			return true;
		}
		return false;
	}

	inline bool GetStatus(XrdCl::XRootDStatus &status)
	{
		if (isValid())
		{
			status = *pStatus;
			return true;
		}
		return false;
	}


	virtual ~FileMappingResponseHandler()
	{
		XrdSysCondVarHelper monitor(pCond);
		if (pStatus)
			delete pStatus;
		if (pResponse)
			delete pResponse;
	}

private:
	XrdCl::XRootDStatus  *pStatus;
	XrdCl::AnyObject     *pResponse;
	int                   pValid;
	int                   pWaitedOnce;
	XrdSysCondVar         pCond;
};

}
