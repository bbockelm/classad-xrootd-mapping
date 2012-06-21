/***************************************************************
 *
 * Copyright (C) 2012, Brian Bockelman
 * University of Nebraska-Lincoln
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ***************************************************************/

#include <pthread.h>
#include <vector>
#include <string>

#include "classad/classad_distribution.h"
#include "classad/classad_stl.h"
#include "classad/fnCall.h"

#include "XrdClient/XrdClientAdmin.hh"

using namespace classad;

static bool files_to_sites(const char *name, ArgumentList const &arguments,
    EvalState &state, Value  &result);

class FileMappingClient;

typedef classad_hash_map<std::string, FileMappingClient*, StringHash> InstanceTable;

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
class FileMappingClient {

public:
	static FileMappingClient &getClient(const std::string &hostname) {
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

	bool map(const std::vector<std::string> &, std::vector<std::string> &) {
		return false;
	}

private:
	FileMappingClient(const std::string &hostname)
		: m_client(("root://" + hostname).c_str())
	{
		m_client.Connect();
	}

	XrdClientAdmin m_client;

	static InstanceTable m_instance_table;
	static pthread_mutex_t m_table_mutex;
};

InstanceTable FileMappingClient::m_instance_table;
pthread_mutex_t FileMappingClient::m_table_mutex = PTHREAD_MUTEX_INITIALIZER;

/***************************************************************************
 *
 * This is the table that maps function names to functions. This is desirable
 * because we don't want all possible functions in your library to be
 * callable from the ClassAd code, but only the ones you want to expose.
 * Also, you can name the functions anything you want. Note that you can
 * list a function twice--it can check to see what it was called when 
 * it was invoked (like a program's argv[0] and change it's behavior. 
 * This can be useful when you have functions that are nearly identical.
 * There are some examples in fnCall.C that do this, like sumAvg and minMax.
 *
 ***************************************************************************/
static ClassAdFunctionMapping functions[] = 
{
    { "filesToSites", (void *) files_to_sites, 0 },
    { "files_to_sites", (void *) files_to_sites, 0 },
    { "",            NULL,                 0 }
};

/***************************************************************************
 * 
 * Required entry point for the library.  This should be the only symbol
 * exported
 *
 ***************************************************************************/
extern "C" 
{

	ClassAdFunctionMapping *Init(void)
	{
		return functions;
	}
}

/****************************************************************************
 *
 * Evaluate in input argument down to a list
 *
 ****************************************************************************/
bool convert_to_vector_string(EvalState & state, Value & arg, std::vector<std::string> &result) {

	ExprList *list_ptr = NULL;

	if (!arg.IsListValue(list_ptr) || !list_ptr)
		return false;

	ExprList &list = *list_ptr;

	result.reserve(list.size());

	for (ExprList::const_iterator it = list.begin(); it != list.end(); it++) {
		Value val;
		const ExprTree * tree = *it;
		if (!tree || !tree->Evaluate(state, val))
			return false;

		std::string string_value;
		if (!val.IsStringValue(string_value))
			return false;

		result.push_back(string_value);
	}

	return true;

}

/****************************************************************************
 *
 * Logs in to Xrootd, using a cached handle if possible.
 *
 ****************************************************************************/
bool login_to_xrootd(const std::string&, XrdClientAdmin*&)
{
	return false;
}

/****************************************************************************
 *
 * Maps a vector of filenames to a set of xrootd hosts.
 *
 ****************************************************************************/
bool map_to_hosts(XrdClientAdmin&, const std::vector<std::string>)
{
	return false;
}

/****************************************************************************
 *
 * Query an xrootd server and translate filenames to locations
 * To use:
 *  files_to_sites("xrootd.example.com", ["file1", "file2", "file3"])
 *
 * This function will aggressively cache the results (matchmaking will call
 * it many times over a short period); it is also guaranteed to return within
 * 50ms.
 *
 * Returns the list of xrootd endpoints which claim to have the file.  Any not
 * responding or not in the cache after 50ms will be left off the list.  It is
 * not possible to distinguish, in this function, the difference between
 * timeouts, failures, and empty responses.
 *
 ****************************************************************************/
static bool files_to_sites(
	const char         *, // name
	const ArgumentList &arguments,
	EvalState          & state,
	Value              &result)
{
	bool    eval_successful = true;

	Value xrootd_host_arg, filenames_arg;
	
	// We check to make sure that we are passed exactly one argument,
	// then we have to evaluate that argument.
	if (arguments.size() != 2) {
		result.SetErrorValue();
		eval_successful = false;
	}

	std::string xrootd_host;
	if (!arguments[0]->Evaluate(state, xrootd_host_arg) || (!xrootd_host_arg.IsStringValue(xrootd_host))) {
		result.SetErrorValue();
		eval_successful = false;
	}

	if (!arguments[1]->Evaluate(state, filenames_arg)) {
		result.SetErrorValue();
		eval_successful = false;
	}
	std::vector<std::string> filenames;
	if (!convert_to_vector_string(state, filenames_arg, filenames)) {
		result.SetErrorValue();
		eval_successful = false;
	}

	XrdClientAdmin *client_ptr;
	if (!login_to_xrootd(xrootd_host, client_ptr) || !client_ptr) {
		result.SetErrorValue();
		eval_successful = false;
	}
	XrdClientAdmin &client = *client_ptr;

	std::vector<std::string> hosts;
	
	map_to_hosts(client, hosts);

	return eval_successful;
}
