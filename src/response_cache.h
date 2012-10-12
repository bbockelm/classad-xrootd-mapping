#ifndef __RESPONSECACHE_H_
#define __RESPONSECACHE_H_

#include <string>
#include "XrdSys/XrdSysPthread.hh"

#include "classad/classad_distribution.h"

namespace ClassadXrootdMapping {

/*
 * This class maintains a cache of Xrootd responses.
 */

class CacheEntry;
class ResponseCache;

typedef classad_unordered<std::string, CacheEntry*> ResponseMap;
/*
 * ClassAds leaks the ExprList object for functions returning lists.
 * Hence, we get to stand on our head to cache all the prior responses.
 * That way, it's memory hoarding, not memory leaks.
 */
typedef classad_unordered<std::string, classad_shared_ptr<classad::ExprList> > ResponseTable;

class CacheEntry {

friend class ResponseCache;

public:

	const std::set<std::string> &getSet() const;

	bool isValid(time_t) const;

	static std::string createHash(const std::set<std::string> & hosts);

protected:

	CacheEntry(const std::string &, const std::set<std::string> &, time_t, ResponseCache &);

private:

	std::string m_filename;
	time_t m_expiration;
	std::set<std::string> m_set;

};

class ResponseCache {

public:

	classad_shared_ptr<classad::ExprList> query(std::vector<std::string> &filename, std::vector<std::string> & files_to_query);

	void insert(const std::string &filename, const std::set<std::string> & hosts);

	static ResponseCache &getInstance();

	static classad_shared_ptr<classad::ExprList> getList(const std::set<std::string> &hosts);
	static void addToList(const std::set<std::string> &hosts, classad_shared_ptr<classad::ExprList>);

private:

	ResponseCache();

	void prune(time_t);

	static const unsigned int m_lifetime_seconds; // The lifetime of each cache entry.
	time_t m_last_pruning;

	ResponseMap m_response_map; // Cache with limited lifetime of entries
	ResponseTable m_response_table; // Permanent table of all ExprLists.

	static ResponseCache * m_instance;
	static XrdSysMutex m_instance_mutex;

	XrdSysMutex m_table_mutex;
};

}

#endif
