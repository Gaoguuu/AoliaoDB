// a journey of a thousand miles begins with a single step
// author: enpeizhao
// blog: www.enpeizhao.com

#include "bpt.h"
#include "TextTable.h"
#include "table_manager.h"
#include <direct.h>		// for _mkdir
#include <sys/stat.h> // for mkdir
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <fstream>
#include <sys/stat.h>
#include <vector>
#include <sstream>

using namespace bpt;
using namespace std;

const char* errorMessage = "> your input is invalid, print \".help\" for more information!\n";
const char* nextLineHeader = "> ";
const char* exitMessage = "> bye!\n";

TableManager* tm = nullptr;

// function prototype
void printHelpMess();
void selectCommand();
void processCreateTable(const string& cmd);
void processInsert(const string& cmd);
void processSelect(const string& cmd);
void processDropTable(const string& cmd);
vector<string> splitString(const string& str, char delimiter);

// initial
void initialSystem()
{
	// step 1: print help message
	printHelpMess();

	// step 2: initialize table manager
	tm = new TableManager("./data/");

	// step 3: REPL select command
	selectCommand();
}

// print help message
void printHelpMess()
{
	cout << "*********************************************************************************************" << endl
		<< endl
		<< "                Welcome to the duck_db\n"
		<< "                Author: enpei\n"
		<< "                www.enpeizhao.com\n"
		<< "                2018-08-31" << endl
		<< endl
		<< "*********************************************************************************************" << endl
		<< "  .help                           print help message;" << endl
		<< "  .exit                           exit program;" << endl
		<< "  CREATE TABLE tablename (field1 TYPE1, field2 TYPE2, ...);   create new table;" << endl
		<< "  DROP TABLE tablename;                                       delete table;" << endl
		<< "  INSERT INTO tablename VALUES (val1, val2, ...);            insert record;" << endl
		<< "  SELECT * FROM tablename;                                   query all records;" << endl
		<< "  SELECT * FROM tablename WHERE condition;                   query with condition;" << endl
		<< "*********************************************************************************************" << endl
		<< endl
		<< nextLineHeader;
}

// select command
void selectCommand()
{
	string cmd;
	while (true)
	{
		getline(cin, cmd);

		if (cmd == ".exit")
		{
			cout << exitMessage;
			break;
		}
		else if (cmd == ".help")
		{
			printHelpMess();
		}
		else if (cmd.find("CREATE TABLE") == 0)
		{
			processCreateTable(cmd);
		}
		else if (cmd.find("INSERT INTO") == 0)
		{
			processInsert(cmd);
		}
		else if (cmd.find("SELECT") == 0)
		{
			processSelect(cmd);
		}
		else if (cmd.find("DROP TABLE") == 0)
		{
			processDropTable(cmd);
		}
		else
		{
			cout << errorMessage << nextLineHeader;
		}
	}
}

void processCreateTable(const string& cmd)
{
	// 解析CREATE TABLE语句
	// 格式: CREATE TABLE tablename (field1 TYPE1(size), field2 TYPE2, ...)
	size_t leftParen = cmd.find('(');
	size_t rightParen = cmd.find_last_of(')');

	if (leftParen == string::npos || rightParen == string::npos)
	{
		cout << errorMessage << nextLineHeader;
		return;
	}

	// 获取表名
	string tableName = cmd.substr(13, leftParen - 13);
	tableName = tableName.substr(0, tableName.find_last_not_of(" ") + 1);

	// 解析字段定义
	string fieldsDef = cmd.substr(leftParen + 1, rightParen - leftParen - 1);
	vector<string> fields = splitString(fieldsDef, ',');

	TableDef def;
	def.tableName = tableName;
	size_t totalSize = 0;

	for (const auto& field : fields)
	{
		istringstream iss(field);
		string fieldName, fieldType;
		size_t fieldSize = 0;

		iss >> fieldName >> fieldType;

		FieldDef fieldDef;
		fieldDef.name = fieldName;
		if (fieldType == "INT")
		{
			fieldDef.type = FieldType::INT;
			fieldDef.size = sizeof(int);
		}
		else if (fieldType.substr(0, 7) == "VARCHAR")
		{
			fieldDef.type = FieldType::VARCHAR;
			size_t sizeStart = fieldType.find('(');
			size_t sizeEnd = fieldType.find(')');
			if (sizeStart != string::npos && sizeEnd != string::npos)
			{
				fieldDef.size = stoi(fieldType.substr(sizeStart + 1, sizeEnd - sizeStart - 1));
			}
			else
			{
				fieldDef.size = 256; // 默认大小
			}
			cout << fieldDef.size << endl;
		}

		totalSize += fieldDef.size;
		def.fields.push_back(fieldDef);
	}

	def.recordSize = totalSize;

	if (tm->createTable(def))
	{
		cout << "> Table created successfully" << nextLineHeader;
	}
	else
	{
		cout << "> Failed to create table" << nextLineHeader;
	}
}

void processInsert(const string& cmd)
{
	// 解析INSERT INTO语句
	// 格式: INSERT INTO tablename VALUES (val1, val2, ...)
	size_t valuesPos = cmd.find("VALUES");
	if (valuesPos == string::npos)
	{
		cout << errorMessage << nextLineHeader;
		return;
	}

	string tableName = cmd.substr(12, valuesPos - 13);

	size_t leftParen = cmd.find('(', valuesPos);
	size_t rightParen = cmd.find(')', valuesPos);

	if (leftParen == string::npos || rightParen == string::npos)
	{
		cout << errorMessage << nextLineHeader;
		return;
	}

	string valuesStr = cmd.substr(leftParen + 1, rightParen - leftParen - 1);
	vector<string> values = splitString(valuesStr, ',');

	// 清理值中的空格和引号
	for (auto& value : values)
	{
		value = value.substr(value.find_first_not_of(" \""));
		value = value.substr(0, value.find_last_not_of(" \"") + 1);
	}

	if (tm->insert(tableName, values))
	{
		cout << "> Record inserted successfully" << nextLineHeader;
	}
	else
	{
		cout << "> Failed to insert record" << nextLineHeader;
	}
}

void processSelect(const string& cmd)
{
	// 解析SELECT语句
	// 格式: SELECT * FROM tablename [WHERE condition]
	size_t fromPos = cmd.find("FROM");
	if (fromPos == string::npos)
	{
		cout << errorMessage << nextLineHeader;
		return;
	}

	size_t wherePos = cmd.find("WHERE");
	string tableName = cmd.substr(fromPos + 5,
		wherePos == string::npos ? string::npos : wherePos - fromPos - 6);

	// 清理表名中的空格
	tableName = tableName.substr(0, tableName.find_last_not_of(" "));

	string whereClause = "";
	if (wherePos != string::npos)
	{
		whereClause = cmd.substr(wherePos + 6);
	}

	auto results = tm->select(tableName, whereClause);

	if (!results.empty())
	{
		// 获取表定义以显示列名
		TableDef def = tm->getTableDef(tableName);

		TextTable t('-', '|', '+');

		// 添加表头
		for (const auto& field : def.fields)
		{
			t.add(" " + field.name + " ");
		}
		t.endOfRow();

		// 添加数据行
		for (const auto& row : results)
		{
			for (const auto& value : row)
			{
				t.add(" " + value + " ");
			}
			t.endOfRow();
		}

		cout << t << nextLineHeader;
	}
	else
	{
		cout << "> No records found" << nextLineHeader;
	}
}

void processDropTable(const string& cmd)
{
	// 解析DROP TABLE语句
	string tableName = cmd.substr(11); // 跳过"DROP TABLE "
	// 清理表名中的空格和分号
	size_t lastChar = tableName.find_last_not_of(" \t\r\n;");
	if (lastChar != string::npos)
	{
		tableName = tableName.substr(0, lastChar + 1);
	}
	// 清理表名前面的空格
	size_t firstChar = tableName.find_first_not_of(" \t\r\n");
	if (firstChar != string::npos)
	{
		tableName = tableName.substr(firstChar);
	}

	if (tm->dropTable(tableName))
	{
		cout << "> Table dropped successfully" << nextLineHeader;
	}
	else
	{
		cout << "> Failed to drop table" << nextLineHeader;
	}
}

vector<string> splitString(const string& str, char delimiter)
{
	vector<string> tokens;
	string token;
	istringstream tokenStream(str);

	while (getline(tokenStream, token, delimiter))
	{
		// 去除首尾空格
		token = token.substr(token.find_first_not_of(" "));
		token = token.substr(0, token.find_last_not_of(" ") + 1);
		if (!token.empty())
		{
			tokens.push_back(token);
		}
	}

	return tokens;
}

void ensure_data_directory()
{
#ifdef _WIN32
	_mkdir(".\\data");
#else
	mkdir("./data", 0777);
#endif
}

int main(int argc, char* argv[])
{
	ensure_data_directory();
	initialSystem();
	delete tm;
	return 0;
}
