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
#include <ctime>

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
void processAlterTable(const string& cmd);
void processDelete(const string& cmd);
void processUpdate(const string& cmd);
pair<string, string> parseSetPair(const string& pairStr);
vector<string> splitString(const string& str, char delimiter);
std::string getCurrentTime();

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
		<< "                Welcome to the AoliaoDB\n"
		<< "                Author: enpei\n"
		<< "                www.enpeizhao.com\n"
		<< "                2024-03-20" << endl
		<< endl
		<< "*********************************************************************************************" << endl
		<< "  .help                           - ��ʾ������Ϣ" << endl
		<< "  .exit                           - �˳�����" << endl
		<< endl
		<< "  �������" << endl
		<< "  CREATE TABLE tablename (                                        - ������" << endl
		<< "    field1 TYPE1(size) [KEY] [NOT NULL] [UNIQUE] [INDEX],       - �ֶζ���" << endl
		<< "    field2 TYPE2 [DEFAULT value],                               - ֧��Ĭ��ֵ" << endl
		<< "    ...                                                         " << endl
		<< "  ) [COMMENT 'table comment'];                                  - ��ע��" << endl
		<< endl
		<< "  ֧�ֵ��������ͣ�" << endl
		<< "    INT                           - ��������" << endl
		<< "    VARCHAR(n)                    - �䳤�ַ�����nΪ��󳤶�" << endl
		<< "    FLOAT                         - �����ȸ�����" << endl
		<< "    DOUBLE                        - ˫���ȸ�����" << endl
		<< "    DATETIME                      - ����ʱ������" << endl
		<< "    BOOL                          - ��������" << endl
		<< endl
		<< "  ���ݲ�����" << endl
		<< "  INSERT INTO tablename VALUES (val1, val2, ...);              - ��������" << endl
		<< "  SELECT * FROM tablename;                                     - ��ѯ��������" << endl
		<< "  SELECT field1, field2 FROM tablename;                        - ��ѯָ���ֶ�" << endl
		<< "  SELECT * FROM tablename WHERE condition;                     - ������ѯ" << endl
		<< "  SELECT t1.field1, t2.field2                                 - ���Ӳ�ѯ" << endl
		<< "    FROM table1 t1 JOIN table2 t2                             " << endl
		<< "    ON t1.field = t2.field;                                   " << endl
		<< endl
		<< "  ��ṹ�޸ģ�" << endl
		<< "  DROP TABLE tablename;                                        - ɾ����" << endl
		<< "  ALTER TABLE tablename ADD COLUMN column type [(size)];       - ����ֶ�" << endl
		<< "  ALTER TABLE tablename DROP COLUMN column;                    - ɾ���ֶ�" << endl
		<< "  ALTER TABLE tablename RENAME COLUMN old TO new;             - �������ֶ�" << endl
		<< "  ALTER TABLE tablename ALTER COLUMN name TYPE newtype;       - �޸��ֶ�����" << endl
		<< "  ALTER TABLE tablename RENAME TO newtable;                   - ��������" << endl
		<< endl
		<< "  �����޸ģ�" << endl
		<< "  DELETE FROM tablename [WHERE condition];                     - ɾ������" << endl
		<< "  UPDATE tablename SET field1=value1 [WHERE condition];       - ��������" << endl
		<< endl
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
		else if (cmd.find("ALTER TABLE") == 0)
		{
			processAlterTable(cmd);
		}
		else if (cmd.find("DELETE FROM") == 0)
		{
			processDelete(cmd);
		}
		else if (cmd.find("UPDATE") == 0)
		{
			processUpdate(cmd);
		}
		else
		{
			cout << errorMessage << nextLineHeader;
		}
	}
}

void processCreateTable(const string& cmd)
{
	// ����CREATE TABLE���
	string fullCmd = cmd;

	// ������û�н�����������ȡ
	while (fullCmd.find(';') == string::npos)
	{
		string line;
		cout << "> ";
		getline(cin, line);
		fullCmd += " " + line;
	}

	// �Ƴ�����Ŀհ��ַ��ͻ��з�
	string::size_type pos = 0;
	while ((pos = fullCmd.find('\n', pos)) != string::npos)
	{
		fullCmd.replace(pos, 1, " ");
	}

	// ��������
	size_t tableNameStart = fullCmd.find("CREATE TABLE") + 12;
	size_t leftParen = fullCmd.find('(');
	if (leftParen == string::npos)
	{
		cout << errorMessage << nextLineHeader;
		return;
	}

	string tableName = fullCmd.substr(tableNameStart, leftParen - tableNameStart);
	// ȥ����β�ո�
	tableName = tableName.substr(tableName.find_first_not_of(" \t"));
	tableName = tableName.substr(0, tableName.find_last_not_of(" \t") + 1);

	// �����ֶζ���
	size_t rightParen = fullCmd.find_last_of(')');
	if (rightParen == string::npos || rightParen < leftParen)
	{
		cout << errorMessage << nextLineHeader;
		return;
	}

	string fieldsDef = fullCmd.substr(leftParen + 1, rightParen - leftParen - 1);
	vector<string> fieldDefs = splitString(fieldsDef, ',');

	TableDef tableDef;
	tableDef.tableName = tableName;
	tableDef.create_time = getCurrentTime();
	tableDef.update_time = tableDef.create_time;

	// ������ע��
	size_t commentPos = fullCmd.find("COMMENT", rightParen);
	if (commentPos != string::npos)
	{
		size_t quoteStart = fullCmd.find('\'', commentPos);
		size_t quoteEnd = fullCmd.find('\'', quoteStart + 1);
		if (quoteStart != string::npos && quoteEnd != string::npos)
		{
			tableDef.comment = fullCmd.substr(quoteStart + 1, quoteEnd - quoteStart - 1);
		}
	}

	// ����ÿ���ֶζ���
	for (const auto& def : fieldDefs)
	{
		istringstream iss(def);
		string fieldName, type;
		iss >> fieldName >> type;

		// �������ֶ�
		if (fieldName.empty() || type.empty())
			continue;

		// �����ֶ�����
		FieldDef field;
		field.name = fieldName;

		// �������ͺʹ�С
		size_t sizeStart = type.find('(');
		if (sizeStart != string::npos)
		{
			size_t sizeEnd = type.find(')');
			string baseType = type.substr(0, sizeStart);
			string sizeStr = type.substr(sizeStart + 1, sizeEnd - sizeStart - 1);
			type = baseType;
			field.size = stoi(sizeStr);
		}

		// �����ֶ�����
		if (type == "INT")
		{
			field.type = FieldType::INT;
			field.size = sizeof(int);
		}
		else if (type == "VARCHAR")
		{
			field.type = FieldType::VARCHAR;
			if (field.size == 0)
				field.size = 256; // Ĭ��VARCHAR����
		}
		else if (type == "FLOAT")
		{
			field.type = FieldType::FLOAT;
			field.size = sizeof(float);
		}
		else if (type == "DOUBLE")
		{
			field.type = FieldType::DOUBLE;
			field.size = sizeof(double);
		}
		else if (type == "DATETIME")
		{
			field.type = FieldType::DATETIME;
			field.size = sizeof(time_t);
		}
		else if (type == "BOOL")
		{
			field.type = FieldType::BOOL;
			field.size = sizeof(bool);
		}
		else
		{
			cout << "> Invalid field type: " << type << nextLineHeader;
			return;
		}

		// �����ֶ�����
		string attr;
		while (iss >> attr)
		{
			if (attr == "KEY")
				field.is_key = true;
			else if (attr == "NOT" && iss >> attr && attr == "NULL")
				field.is_nullable = false;
			else if (attr == "UNIQUE")
				field.is_unique = true;
			else if (attr == "INDEX")
				field.is_index = true;
			else if (attr == "DEFAULT" && iss >> attr)
			{
				// �Ƴ�����
				if (attr.front() == '\'' || attr.front() == '"')
					attr = attr.substr(1, attr.length() - 2);
				field.default_value = attr;
			}
		}

		tableDef.fields.push_back(field);
	}

	if (tm->createTable(tableDef))
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
	// ����INSERT INTO���
	string fullCmd = cmd;

	// ������û�н�����������ȡ
	while (fullCmd.find(';') == string::npos)
	{
		string line;
		cout << "> ";
		getline(cin, line);
		fullCmd += " " + line;
	}

	// ��ȡ������ֵ�б�
	size_t valuesPos = fullCmd.find("VALUES");
	if (valuesPos == string::npos)
	{
		cout << errorMessage << nextLineHeader;
		return;
	}

	string tableName = fullCmd.substr(12, valuesPos - 12); // ����"INSERT INTO "
	tableName = tableName.substr(0, tableName.find_last_not_of(" \t") + 1);

	// ����ֵ�б�
	size_t leftParen = fullCmd.find('(', valuesPos);
	size_t rightParen = fullCmd.find(')', leftParen);
	if (leftParen == string::npos || rightParen == string::npos)
	{
		cout << errorMessage << nextLineHeader;
		return;
	}

	string valuesList = fullCmd.substr(leftParen + 1, rightParen - leftParen - 1);
	vector<string> values;
	string currentValue;
	bool inQuotes = false;
	char quoteChar = 0;

	// �����ܵ�ֵ����
	for (size_t i = 0; i < valuesList.length(); i++)
	{
		char c = valuesList[i];
		if ((c == '\'' || c == '"') && (!inQuotes || c == quoteChar))
		{
			if (!inQuotes)
				quoteChar = c;
			inQuotes = !inQuotes;
			currentValue += c;
		}
		else if (c == ',' && !inQuotes)
		{
			// ȥ����β�ո�
			currentValue = currentValue.substr(currentValue.find_first_not_of(" \t"));
			currentValue = currentValue.substr(0, currentValue.find_last_not_of(" \t") + 1);
			if (!currentValue.empty())
				values.push_back(currentValue);
			currentValue.clear();
		}
		else
		{
			currentValue += c;
		}
	}

	// ������һ��ֵ
	if (!currentValue.empty())
	{
		currentValue = currentValue.substr(currentValue.find_first_not_of(" \t"));
		currentValue = currentValue.substr(0, currentValue.find_last_not_of(" \t") + 1);
		values.push_back(currentValue);
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
	// ��ȡ������ SQL ���
	string fullCmd = cmd;
	while (fullCmd.find(';') == string::npos)
	{
		string line;
		cout << "> ";
		getline(cin, line);
		fullCmd += " " + line;
	}

	// �Ƴ�����Ŀհ��ַ��ͻ��з�
	string::size_type pos = 0;
	while ((pos = fullCmd.find('\n', pos)) != string::npos)
	{
		fullCmd.replace(pos, 1, " ");
	}

	// �Ƴ�ĩβ�ķֺ�
	fullCmd = fullCmd.substr(0, fullCmd.find_last_not_of(" ;") + 1);

	istringstream iss(fullCmd);
	string select, fields_str, from, tableName, where_clause;
	vector<string> fields;

	// ��ȡ SELECT �ؼ���
	iss >> select;

	// ��ȡ�ֶ��б�ֱ�� FROM
	string temp;
	getline(iss >> std::ws, temp, 'F');
	fields_str = temp;

	// ��ȡ FROM ���������
	getline(iss >> std::ws, temp);
	string rest = "F" + temp; // �� F �ӻ�ȥ����Ϊ֮ǰ���ָ����

	// ����Ƿ�������
	if (rest.find("JOIN") != string::npos)
	{
		// �����������
		istringstream tabless(rest);
		string table1, join, table2, on, joinCondition;

		// ���� "FROM "
		tabless >> from >> table1 >> join >> table2 >> on;

		// ��ȡ��������
		getline(tabless >> std::ws, joinCondition);

		// �������������
		table1 = table1.substr(table1.find_first_not_of(" \t"));
		table1 = table1.substr(0, table1.find_last_not_of(" \t") + 1);
		table2 = table2.substr(table2.find_first_not_of(" \t"));
		table2 = table2.substr(0, table2.find_last_not_of(" \t") + 1);

		// �����ֶ�
		fields_str = fields_str.substr(fields_str.find_first_not_of(" \t"));
		fields_str = fields_str.substr(0, fields_str.find_last_not_of(" \t") + 1);

		if (fields_str != "*")
		{
			istringstream fieldss(fields_str);
			string field;
			while (getline(fieldss, field, ','))
			{
				field = field.substr(field.find_first_not_of(" \t"));
				field = field.substr(0, field.find_last_not_of(" \t") + 1);
				fields.push_back(field);
			}
		}

		// ִ�����Ӳ�ѯ
		auto results = tm->selectJoin(table1, table2, joinCondition, fields);

		// ��ʾ���
		if (!results.empty())
		{
			TextTable t;

			// ��ӱ�ͷ
			if (fields.empty())
			{
				// ��ȡ������Ķ���
				TableDef def1 = tm->getTableDef(table1);
				TableDef def2 = tm->getTableDef(table2);

				// ��ӵ�һ������ֶ�
				for (const auto& field : def1.fields)
				{
					t.add(" " + table1 + "." + field.name + " ");
				}

				// ��ӵڶ�������ֶ�
				for (const auto& field : def2.fields)
				{
					t.add(" " + table2 + "." + field.name + " ");
				}
			}
			else
			{
				// ʹ��ָ�����ֶ�
				for (const auto& field : fields)
				{
					t.add(" " + field + " ");
				}
			}
			t.endOfRow();

			// ���������
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
	else
	{
		// ���������ѯ
		istringstream rest_ss(rest);
		rest_ss >> from >> tableName;

		// ����Ƿ��� WHERE �Ӿ�
		string where;
		if (rest_ss >> where && where == "WHERE")
		{
			// ��ȡʣ�������������Ϊ����
			getline(rest_ss >> std::ws, where_clause);
		}

		// �����ֶ��б�
		fields_str = fields_str.substr(fields_str.find_first_not_of(" \t"));
		fields_str = fields_str.substr(0, fields_str.find_last_not_of(" \t") + 1);

		// ������� * ������ֶ��б�
		if (fields_str != "*")
		{
			istringstream fields_stream(fields_str);
			string field;
			while (getline(fields_stream, field, ','))
			{
				field = field.substr(field.find_first_not_of(" \t"));
				field = field.substr(0, field.find_last_not_of(" \t") + 1);
				fields.push_back(field);
			}
		}

		// ִ�в�ѯ
		vector<vector<string>> results;
		if (fields_str == "*")
		{
			// ��ѯ�����ֶ�
			results = tm->select(tableName, vector<string>(), where_clause);
		}
		else
		{
			// ��ѯָ���ֶ�
			results = tm->select(tableName, fields, where_clause);
		}

		// ��ȡ��������ʾ�ֶ���
		TableDef def = tm->getTableDef(tableName);

		// ��ʾ���
		if (!results.empty())
		{
			TextTable t;

			// ��ӱ�ͷ
			if (fields.empty())
			{
				// ��ʾ�����ֶ�
				for (const auto& field : def.fields)
				{
					t.add(" " + field.name + " ");
				}
			}
			else
			{
				// ��ʾѡ�����ֶ�
				for (const auto& field : fields)
				{
					t.add(" " + field + " ");
				}
			}
			t.endOfRow();

			// ���������
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
}

void processDropTable(const string& cmd)
{
	// ����DROP TABLE���
	string tableName = cmd.substr(11); // ����"DROP TABLE "
	// ��������еĿո�ͷֺ�
	size_t lastChar = tableName.find_last_not_of(" \t\r\n;");
	if (lastChar != string::npos)
	{
		tableName = tableName.substr(0, lastChar + 1);
	}
	// �������ǰ��Ŀո�
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

void processAlterTable(const string& cmd)
{
	istringstream iss(cmd);
	string alter, table, tableName, operation;
	iss >> alter >> table >> tableName >> operation;

	// ����������Ƴ��ո�ͷֺţ�
	tableName = tableName.substr(0, tableName.find_last_not_of(" ;") + 1);
	tableName = tableName.substr(tableName.find_first_not_of(" "));

	if (operation == "RENAME")
	{
		string column, oldName, to, newName;
		iss >> column;

		if (column == "COLUMN")
		{
			// RENAME COLUMN oldname TO newname
			iss >> oldName >> to >> newName;

			// �����ֶ������Ƴ��ո�ͷֺţ�
			newName = newName.substr(0, newName.find_last_not_of(" ;") + 1);

			if (to == "TO" && !oldName.empty() && !newName.empty())
			{
				if (tm->renameField(tableName, oldName, newName))
				{
					cout << "> Column renamed successfully" << nextLineHeader;
				}
				else
				{
					cout << "> Failed to rename column" << nextLineHeader;
				}
				return;
			}
		}
		else if (column == "TO")
		{
			// RENAME TO newname
			string newTableName;
			iss >> newTableName;

			// �����±������Ƴ��ո�ͷֺţ�
			newTableName = newTableName.substr(0, newTableName.find_last_not_of(" ;") + 1);

			if (tm->renameTable(tableName, newTableName))
			{
				cout << "> Table renamed successfully" << nextLineHeader;
			}
			else
			{
				cout << "> Failed to rename table" << nextLineHeader;
			}
			return;
		}
	}
	else if (operation == "DROP")
	{
		string column, columnName;
		iss >> column;
		if (column == "COLUMN")
		{
			iss >> columnName;
			// �����ֶ������Ƴ��ո�ͷֺţ�
			columnName = columnName.substr(0, columnName.find_last_not_of(" ;") + 1);

			if (tm->dropField(tableName, columnName))
			{
				cout << "> Column dropped successfully" << nextLineHeader;
			}
			else
			{
				cout << "> Failed to drop column" << nextLineHeader;
			}
			return;
		}
	}
	else if (operation == "ADD")
	{
		// ADD COLUMN name type [(size)]
		string column;
		string columnName;
		string type;

		iss >> column;
		if (column != "COLUMN")
		{
			cout << "> Expected 'COLUMN' keyword" << nextLineHeader;
			return;
		}

		// ��ȡ����������
		if (!(iss >> columnName >> type))
		{
			cout << "> Invalid column definition" << nextLineHeader;
			return;
		}

		// ���������ַ���
		type = type.substr(0, type.find_last_not_of(" ;"));

		// �������ͺʹ�С
		FieldType fieldType;
		size_t size = 0;

		if (type == "INT")
		{
			fieldType = FieldType::INT;
		}
		else if (type.substr(0, 7) == "VARCHAR")
		{
			fieldType = FieldType::VARCHAR;
			size_t start = type.find('(');
			size_t end = type.find(')');
			if (start != string::npos && end != string::npos)
			{
				try
				{
					size = stoi(type.substr(start + 1, end - start - 1));
				}
				catch (const std::exception& e)
				{
					cout << "> Invalid VARCHAR size" << nextLineHeader;
					return;
				}
			}
			else
			{
				size = 256; // Ĭ�� VARCHAR ��С
			}
		}
		else if (type == "FLOAT")
		{
			fieldType = FieldType::FLOAT;
		}
		else if (type == "DOUBLE")
		{
			fieldType = FieldType::DOUBLE;
		}
		else if (type == "DATETIME")
		{
			fieldType = FieldType::DATETIME;
		}
		else if (type == "BOOL")
		{
			fieldType = FieldType::BOOL;
		}
		else
		{
			cout << "> Invalid field type: '" << type << "'" << nextLineHeader;
			return;
		}

		// �Ȼ�ȡ����
		TableDef def = tm->getTableDef(tableName);
		if (def.tableName.empty())
		{
			cout << "> Table not found: " << tableName << nextLineHeader;
			return;
		}

		// ����ֶ����Ƿ��Ѵ���
		for (const auto& field : def.fields)
		{
			if (field.name == columnName)
			{
				cout << "> Column already exists: " << columnName << nextLineHeader;
				return;
			}
		}

		if (tm->addField(tableName, columnName, fieldType, size))
		{
			cout << "> Column added successfully" << nextLineHeader;
		}
		else
		{
			cout << "> Failed to add column" << nextLineHeader;
		}
	}
	else if (operation == "ALTER")
	{
		// ALTER COLUMN name TYPE newtype [(size)]
		string column;
		string columnName;
		string type;

		iss >> column;
		if (column != "COLUMN")
		{
			cout << "> Expected 'COLUMN' keyword" << nextLineHeader;
			return;
		}

		string newType;
		iss >> newType;
		if (newType.empty())
		{
			cout << "> Invalid type specification" << nextLineHeader;
			return;
		}

		FieldType fieldType;
		size_t size = 0;

		// ���������ͺʹ�С
		if (newType == "INT")
		{
			fieldType = FieldType::INT;
		}
		else if (newType.substr(0, 7) == "VARCHAR")
		{
			fieldType = FieldType::VARCHAR;
			size_t start = newType.find('(');
			size_t end = newType.find(')');
			if (start != string::npos && end != string::npos)
			{
				try
				{
					size = stoi(newType.substr(start + 1, end - start - 1));
				}
				catch (const std::exception& e)
				{
					cout << "> Invalid VARCHAR size" << nextLineHeader;
					return;
				}
			}
			else
			{
				size = 256; // Ĭ�� VARCHAR ��С
			}
		}
		else if (newType == "FLOAT")
		{
			fieldType = FieldType::FLOAT;
		}
		else if (newType == "DOUBLE")
		{
			fieldType = FieldType::DOUBLE;
		}
		else if (newType == "DATETIME")
		{
			fieldType = FieldType::DATETIME;
		}
		else if (newType == "BOOL")
		{
			fieldType = FieldType::BOOL;
		}
		else
		{
			cout << "> Invalid field type: '" << newType << "'" << nextLineHeader;
			return;
		}

		if (tm->alterFieldType(tableName, columnName, fieldType, size))
		{
			cout << "> Column type altered successfully" << nextLineHeader;
		}
		else
		{
			cout << "> Failed to alter column type" << nextLineHeader;
		}
	}
	else
	{
		cout << "> Invalid ALTER TABLE operation" << nextLineHeader;
	}
}

void processDelete(const string& cmd)
{
	// ����DELETE���
	// DELETE FROM tablename [WHERE condition];
	string tableName = cmd.substr(12); // ����"DELETE FROM "
	string where;

	size_t wherePos = tableName.find("WHERE");
	if (wherePos != string::npos)
	{
		where = tableName.substr(wherePos + 6);
		tableName = tableName.substr(0, wherePos);
	}

	// ȥ��ĩβ�ķֺźͿո�
	tableName = tableName.substr(0, tableName.find_last_not_of(" ;") + 1);

	if (tm->deleteRecords(tableName, where))
	{
		cout << "> Records deleted successfully" << nextLineHeader;
	}
	else
	{
		cout << "> Failed to delete records" << nextLineHeader;
	}
}

pair<string, string> parseSetPair(const string& pairStr)
{
	size_t equalPos = pairStr.find('=');
	if (equalPos == string::npos)
		return { "", "" };

	string field = pairStr.substr(0, equalPos);
	string value = pairStr.substr(equalPos + 1);

	// ����ո�
	field = field.substr(field.find_first_not_of(" \t"));
	field = field.substr(0, field.find_last_not_of(" \t") + 1);
	value = value.substr(value.find_first_not_of(" \t"));
	value = value.substr(0, value.find_last_not_of(" \t") + 1);

	return { field, value };
}

void processUpdate(const string& cmd)
{
	// ����UPDATE���
	string tableName = cmd.substr(7); // ����"UPDATE "
	size_t setPos = tableName.find("SET");
	if (setPos == string::npos)
	{
		cout << errorMessage << nextLineHeader;
		return;
	}

	string tableNameStr = tableName.substr(0, setPos);
	string rest = tableName.substr(setPos + 4);

	vector<pair<string, string>> setValues;
	string where;

	size_t wherePos = rest.find("WHERE");
	string setClause = wherePos != string::npos ? rest.substr(0, wherePos) : rest;
	if (wherePos != string::npos)
	{
		where = rest.substr(wherePos + 6);
	}

	// ����SET�Ӿ䣬֧�������е�ֵ
	string currentPair;
	bool inQuotes = false;
	char quoteChar = 0;

	for (char c : setClause)
	{
		if ((c == '\'' || c == '"') && (!inQuotes || c == quoteChar))
		{
			inQuotes = !inQuotes;
			if (!inQuotes)
				quoteChar = 0;
			else
				quoteChar = c;
			currentPair += c;
		}
		else if (c == ',' && !inQuotes)
		{
			setValues.push_back(parseSetPair(currentPair));
			currentPair.clear();
		}
		else
		{
			currentPair += c;
		}
	}

	if (!currentPair.empty())
	{
		setValues.push_back(parseSetPair(currentPair));
	}

	if (tm->update(tableNameStr, setValues, where))
	{
		cout << "> Records updated successfully" << nextLineHeader;
	}
	else
	{
		cout << "> Failed to update records" << nextLineHeader;
	}
}

vector<string> splitString(const string& str, char delimiter)
{
	vector<string> tokens;
	string token;
	istringstream tokenStream(str);

	while (getline(tokenStream, token, delimiter))
	{
		// ȥ����β�ո�
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

std::string getCurrentTime()
{
	time_t now = time(nullptr);
	char buf[64];
#ifdef _WIN32
	struct tm timeinfo;
	localtime_s(&timeinfo, &now);
	strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &timeinfo);
#else
	strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", localtime(&now));
#endif
	return std::string(buf);
}

int main(int argc, char* argv[])
{
	ensure_data_directory();
	initialSystem();
	delete tm;
	return 0;
}
