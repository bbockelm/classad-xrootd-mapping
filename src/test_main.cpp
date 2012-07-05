
#include <iostream>
#include <sstream>
#include <fstream>
#include <algorithm>
#include <iterator>
#include <string>

#include "classad/classad_distribution.h"

using namespace classad;

int main(int argc, char* argv[]) {

	if (argc != 3) {
		std::cout << "Usage: ./test_main <loadable classad module filename> <classad_filename>" << std::endl;
		return 1;
	}

	if (!FunctionCall::RegisterSharedLibraryFunctions(argv[1]))
	{
		std::cout << "Failed to load ClassAd user lib (" << argv[1] << "): " << classad::CondorErrMsg << std::endl;
		return 1;
	}

	std::stringstream ss;
	std::ifstream ifs(argv[2], std::ifstream::in);
	if (ifs)
	{
		copy(std::istreambuf_iterator<char>(ifs),
			std::istreambuf_iterator<char>(),
			std::ostreambuf_iterator<char>(ss));
		ifs.close();
	} else
	{
		std::cout << "Unable to open file." <<std::endl;
		return 1;
	}

	std::string classad_str = ss.str();

	ClassAd classad;
	ClassAdParser parser;

	PrettyPrint pp;
	bool success = parser.ParseClassAd(classad_str, classad, true);
	if (success)
	{
		std::string unparsed;
		pp.Unparse(unparsed, &classad);
		std::cout << "Resulting ClassAd:" << std::endl << unparsed << std::endl << std::endl;
	}
	else
	{
		std::cout << "Unable to parse ClassAd." << std::endl;
	}

	Value val;
	if (!classad.EvaluateAttr("sites", val) || val.IsErrorValue())
	{
		std::string unparsed;
		ExprTree *tree;
		tree = classad.Lookup("sites");
		if (tree)
		{	
			pp.Unparse(unparsed, tree);
			std::cout << "Unable to evaluate the 'sites' attribute: " << unparsed << std::endl;
			if (CondorErrMsg.size())
			{
				std::cout << "Error message: " << CondorErrMsg << std::endl;
			}
		}
		else
		{
			std::cout << "Attributes 'sites' not present in classad." << std::endl;
		}
		return 1;
	}
	else
	{
		std::string unparsed;
		pp.Unparse(unparsed, val);
		std::cout << "Value of 'sites' attribute: " << unparsed << std::endl;
	}

	if (!classad.EvaluateAttr("sites", val) || val.IsErrorValue())
	{
		std::cout<< "Unable to evaluate the 'sites' attribute a second time!" << std::endl;
		if (CondorErrMsg.size())
		{
			std::cout << "Error message: " << CondorErrMsg << std::endl;
		}
		return 1;
	}
	else
	{
		std::string unparsed;
		pp.Unparse(unparsed, val);
		std::cout << "Value of re-evaluated 'sites' attribute: " << unparsed << std::endl;
	}

	return 0;
}
