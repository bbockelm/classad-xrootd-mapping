
#include <sstream>
#include <vector>

#include "XrdSys/XrdSysPthread.hh"
#include "response_cache.h"

using namespace classad;
using namespace ClassadXrootdMapping;

ResponseCache * ResponseCache::m_instance = NULL;
XrdSysMutex ResponseCache::m_instance_mutex;

const unsigned int ResponseCache::m_lifetime_seconds = 15*60; // defaults to 15 minutes.

CacheEntry::CacheEntry(const std::string & filename, const std::set<std::string> &hosts, time_t expiration, ResponseCache &cache)
	: m_filename(filename),
	  m_expiration(expiration)
{
	m_set.insert(hosts.begin(), hosts.end());
}

const std::set<std::string> &
CacheEntry::getSet() const
{
	return m_set;
}

bool
CacheEntry::isValid(time_t now) const
{
	return now < m_expiration;
}

ResponseCache::ResponseCache() :
	m_last_pruning(time(0)),
	m_table_mutex()
{}

ResponseCache &
ResponseCache::getInstance()
{
	XrdSysMutexHelper monitor(m_instance_mutex);

	if (m_instance == NULL) {
		m_instance = new ResponseCache();
	}
	return *m_instance;
}

classad_shared_ptr<ExprList>
ResponseCache::query(std::vector<std::string> &filenames, std::vector<std::string> &files_remaining)
{
	time_t now = time(NULL);

	// Always prune, since we can't do it automatically
	prune(now);

	std::set<std::string> hosts;
	{
	XrdSysMutexHelper monitor(m_instance_mutex);

	for (std::vector<std::string>::const_iterator it = filenames.begin(); it != filenames.end(); ++it)
	{
		ResponseMap::iterator map_it = m_response_map.find(*it);
		if (map_it == m_response_map.end()) {
			files_remaining.push_back(*it);
			continue;
		}

		const CacheEntry & entry = *(map_it->second);
		if (!entry.isValid(now))
		{
			files_remaining.push_back(*it);
			continue;
		}

		const std::set<std::string> &file_hosts = entry.getSet();
		hosts.insert(file_hosts.begin(), file_hosts.end());
	}
	}

	return getList(hosts);
}

void
ResponseCache::prune(time_t now)
{
	XrdSysMutexHelper monitor(m_instance_mutex);

	// Do not prune too frequently.
	if (now - m_last_pruning < 60)
	{
		return;
	}

	ResponseMap::iterator it;
	for (it = m_response_map.begin(); it != m_response_map.end(); ++it)
	{
		if (!it->second->isValid(now))
		{
			delete it->second;
			it->second = NULL;
			m_response_map.erase(it);
		}
	}
}

void
ResponseCache::insert(const std::string &filename, const std::set<std::string> & hosts)
{
	XrdSysMutexHelper monitor(m_instance_mutex);

	time_t now = time(NULL);
	CacheEntry *entry = new CacheEntry(filename, hosts, now+m_lifetime_seconds, *this);
	m_response_map[filename] = entry;
}

classad_shared_ptr<ExprList>
ResponseCache::getList(const std::set<std::string> &hosts)
{

	ExprList *expr_list_ptr = new ExprList();
	for (std::set<std::string>::const_iterator it = hosts.begin(); it != hosts.end(); ++it)
	{
		Value v;
		v.SetStringValue(*it);
		expr_list_ptr->push_back(Literal::MakeLiteral(v));
	}

	classad_shared_ptr<ExprList> expr_list;
	expr_list.reset(expr_list_ptr);
	return expr_list;
}

void
ResponseCache::addToList(const std::set<std::string> &hosts, classad_shared_ptr<ExprList> expr_list)
{
	for (std::set<std::string>::const_iterator it = hosts.begin(); it != hosts.end(); ++it)
	{
		Value v;
		v.SetStringValue(*it);
		expr_list->push_back(Literal::MakeLiteral(v));
	}
}

