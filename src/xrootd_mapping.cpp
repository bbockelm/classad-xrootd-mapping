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

#include <vector>
#include <string>
#include <sstream>

#include "classad/classad_distribution.h"
#include "classad/classad_stl.h"
#include "classad/fnCall.h"

#include "xrootd_client.h"

using namespace classad;

static bool files_to_sites(const char *name, ArgumentList const &arguments,
    EvalState &state, Value  &result);


/*
 * ClassAds leaks the ExprList object for functions returning lists.
 * Hence, we get to stand on our head to cache all the prior responses.
 * That way, it's memory hoarding, not memory leaks.
 */
typedef classad_hash_map<std::string, ExprList*, StringHash> ResponseTable;
static ResponseTable response_cache;

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
	Value xrootd_host_arg, filenames_arg;
	
	// We check to make sure that we are passed exactly one argument,
	// then we have to evaluate that argument.
	if (arguments.size() != 2) {
		result.SetErrorValue();
		CondorErrMsg = "Invalid number of arguments passed to files_to_sites; 2 required.";
		return false;
	}

	std::string xrootd_host;
	if (!arguments[0]->Evaluate(state, xrootd_host_arg) || (!xrootd_host_arg.IsStringValue(xrootd_host))) {
		result.SetErrorValue();
		CondorErrMsg = "Could not evaluate the first argument (Xrootd hostname) of files_to_sites to a string.";
		return false;
	}

	if (!arguments[1]->Evaluate(state, filenames_arg)) {
		result.SetErrorValue();
		CondorErrMsg = "Could not evaluate the second argument (list of filenames) of files_to_sites.";
		return false;
	}
	std::vector<std::string> filenames;
	std::string single_filename;
	if (filenames_arg.IsStringValue(single_filename))
	{
		filenames.push_back(single_filename);
	}
	else if (!convert_to_vector_string(state, filenames_arg, filenames))
	{
		result.SetErrorValue();
		CondorErrMsg = "Could not evaluate the second argument (list of filenames) of files_to_sites to a list of strings.";
		return false;
	}

	FileMappingClient &client = FileMappingClient::getClient(xrootd_host);
	if (!client.is_connected()) {
		result.SetErrorValue();
		CondorErrMsg = "Could not connect to specified xrootd host: " + xrootd_host;
		return false;
	}

	std::vector<std::string> hosts;
	if (!client.map(filenames, hosts)) {
		result.SetErrorValue();
		CondorErrMsg = "Error while mapping the files to hosts.";
		return false;
	}

	ExprList * result_list = new ExprList();
	std::stringstream ss;
	sort(hosts.begin(), hosts.end());
	for (std::vector<std::string>::const_iterator it=hosts.begin(); it!=hosts.end(); ++it) {
		Value v;
		v.SetStringValue(*it);
		result_list->push_back(Literal::MakeLiteral(v));
		ss << *it << ",";
	}

	std::string response_hash = ss.str();
	ResponseTable::iterator cache_contents = response_cache.find(response_hash);
    if (cache_contents != response_cache.end()) {
		result.SetListValue(cache_contents->second);
	}
	else
	{
		ExprList * result_list = new ExprList();
		for (std::vector<std::string>::const_iterator it=hosts.begin(); it!=hosts.end(); ++it) {
			Value v;
			v.SetStringValue(*it);
			result_list->push_back(Literal::MakeLiteral(v));
		}
		result.SetListValue(result_list);
		response_cache[response_hash] = NULL;
	}

	return true;
}

