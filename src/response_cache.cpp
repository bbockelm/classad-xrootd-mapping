
#include <sstream>
#include <vector>

#include "pthread_utils.h"
#include "response_cache.h"

using namespace classad;
using namespace ClassadXrootdMapping;

ResponseCache * ResponseCache::m_instance = NULL;
pthread_mutex_t ResponseCache::m_instance_mutex = PTHREAD_MUTEX_INITIALIZER;

const unsigned int ResponseCache::m_lifetime_seconds = 15*60; // defaults to 15 minutes.

std::string
CacheEntry::createHash(const std::set<std::string> &hosts)
{
	std::vector<std::string> hosts_sorted;
	hosts_sorted.reserve(hosts.size());
	for (std::set<std::string>::const_iterator it = hosts.begin(); it != hosts.end(); ++it)
	{
		hosts_sorted.push_back(*it);
	}
	sort(hosts_sorted.begin(), hosts_sorted.end());

	std::stringstream ss;
	for (std::vector<std::string>::const_iterator it=hosts_sorted.begin(); it!=hosts_sorted.end(); ++it) {
		ss << *it << ",";
	}

	return ss.str();
}

CacheEntry::CacheEntry(const std::string & filename, const std::set<std::string> &hosts, time_t expiration, ResponseCache &cache)
	: m_filename(filename),
	  m_expiration(expiration)
{

	ExprList * expr_list_ptr = new ExprList();
	classad_shared_ptr<ExprList> empty_list;
	empty_list.reset(expr_list_ptr);
	classad_shared_ptr<ExprList> expr_list = cache.addToList(empty_list, hosts);

	m_list_hash = createHash(hosts);	
}

const std::set<std::string> &
CacheEntry::getSet() const
{
	return m_set;
}

std::string
CacheEntry::getListHash() const
{
	return m_list_hash;
}

classad_shared_ptr<ExprList>
CacheEntry::getList() const
{
	return m_expr_list;
}

bool
CacheEntry::isValid(time_t now) const
{
	return now < m_expiration;
}

ResponseCache::ResponseCache()
{
}

ResponseCache &
ResponseCache::getInstance()
{
	Lock monitor(m_instance_mutex);

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

	Lock monitor(m_instance_mutex);

	std::set<std::string> hosts;
	for (std::vector<std::string>::const_iterator it = filenames.begin(); it != filenames.end(); ++it)
	{
		ResponseMap::iterator map_it = m_response_map.find(*it);
		if (map_it == m_response_map.end())
			continue;

		const CacheEntry & entry = *(map_it->second);
		if (!entry.isValid(now))
		{
			files_remaining.push_back(*it);
			continue;
		}

		const std::set<std::string> &file_hosts = entry.getSet();
		hosts.insert(file_hosts.begin(), file_hosts.end());
	}
	classad_shared_ptr<ExprList> empty_list;
	return addToList(empty_list, hosts);
}

void
ResponseCache::prune(time_t now)
{
	Lock monitor(m_instance_mutex);

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
	time_t now = time(NULL);
	CacheEntry *entry = new CacheEntry(filename, hosts, now, *this);
	m_response_map[filename] = entry;
}

classad_shared_ptr<ExprList>
ResponseCache::getList(const std::set<std::string> &hosts)
{
	std::string hash = CacheEntry::createHash(hosts);

	ResponseTable::iterator it = m_response_table.find(hash);
	if (it != m_response_table.end())
		return it->second;

	ExprList *expr_list_ptr = new ExprList();
	for (std::set<std::string>::const_iterator it = hosts.begin(); it != hosts.end(); ++it)
	{
		Value v;
		v.SetStringValue(*it);
		expr_list_ptr->push_back(Literal::MakeLiteral(v));
	}

	classad_shared_ptr<ExprList> expr_list;
	expr_list.reset(expr_list_ptr);
	m_response_table[hash] = expr_list;
	return expr_list;
}
