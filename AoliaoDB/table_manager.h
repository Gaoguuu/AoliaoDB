#pragma once
#include "bpt.h"
#include "table_def.h"
#include <map>

class TableManager
{
public:
    TableManager(const std::string& dbPath);
    ~TableManager();

    bool createTable(const TableDef& tableDef);
    bool dropTable(const std::string& tableName);
    TableDef getTableDef(const std::string& tableName);
    bool insert(const std::string& tableName, const std::vector<std::string>& values);
    std::vector<std::vector<std::string>> select(
        const std::string& tableName,
        const std::vector<std::string>& fields = std::vector<std::string>(),
        const std::string& where = "");

    bool addField(const std::string& tableName, const std::string& fieldName,
        FieldType type, size_t size = 0);
    bool dropField(const std::string& tableName, const std::string& fieldName);
    bool renameField(const std::string& tableName, const std::string& oldName,
        const std::string& newName);
    bool alterFieldType(const std::string& tableName, const std::string& fieldName,
        FieldType newType, size_t newSize = 0);

    bool deleteRecords(const std::string& tableName, const std::string& where = "");
    bool update(const std::string& tableName,
        const std::vector<std::pair<std::string, std::string>>& setValues,
        const std::string& where = "");

    bool renameTable(const std::string& oldName, const std::string& newName);

    std::vector<std::vector<std::string>> selectJoin(
        const std::string& table1,
        const std::string& table2,
        const std::string& joinCondition,
        const std::vector<std::string>& fields = std::vector<std::string>(),
        const std::string& where = "");

private:
    std::string dbPath;
    std::map<std::string, bpt::bplus_tree*> tables;
    std::map<std::string, TableDef> tableDefs;

    struct Condition
    {
        std::string field;
        std::string op;
        std::string value;
    };

    bpt::value_t serializeValues(const TableDef& def,
        const std::vector<std::string>& values);
    std::vector<std::string> deserializeValues(const TableDef& def,
        const bpt::value_t& data);
    void saveTableDefs();
    void loadTableDefs();
    bool rebuildTable(const std::string& tableName,
        const TableDef& oldDef,
        const TableDef& newDef,
        const std::string& oldFieldName = "",
        const std::string& newFieldName = "");
    std::vector<Condition> parseWhereClause(const std::string& where);
    bool matchConditions(const std::vector<std::string>& record,
        const TableDef& def,
        const std::vector<Condition>& conditions);
    int getFieldIndex(const TableDef& def, const std::string& fieldName);

    struct JoinCondition
    {
        std::string table1Field;
        std::string table2Field;
    };

    JoinCondition parseJoinCondition(const std::string& condition);
};