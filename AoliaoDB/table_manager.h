#pragma once
#include "bpt.h"
#include "table_def.h"
#include <map>

class TableManager
{
public:
    TableManager(const std::string& dbPath);

    // �����±�
    bool createTable(const TableDef& tableDef);

    // ɾ����
    bool dropTable(const std::string& tableName);

    // ��ȡ����
    TableDef getTableDef(const std::string& tableName);

    // �����¼
    bool insert(const std::string& tableName, const std::vector<std::string>& values);

    // ��ѯ��¼
    std::vector<std::vector<std::string>> select(const std::string& tableName,
        const std::string& where = "");

    ~TableManager()
    {
        // �������д򿪵ı�
        for (auto& pair : tables)
        {
            if (pair.second)
            {
                delete pair.second;
                pair.second = nullptr;
            }
        }
        tables.clear();
        tableDefs.clear();
    }

private:
    std::string dbPath;
    std::map<std::string, bpt::bplus_tree*> tables;
    std::map<std::string, TableDef> tableDefs;

    // ���ַ���ֵת��Ϊ�����Ƹ�ʽ
    bpt::value_t serializeValues(const TableDef& def,
        const std::vector<std::string>& values);

    // �������Ƹ�ʽת�����ַ���ֵ
    std::vector<std::string> deserializeValues(const TableDef& def,
        const bpt::value_t& data);

    // ������嵽�ļ�
    void saveTableDefs();

    // ���ļ����ر���
    void loadTableDefs();
};