#pragma once
#include "bpt.h"
#include "table_def.h"
#include <map>

class TableManager
{
public:
    TableManager(const std::string& dbPath);

    // 创建新表
    bool createTable(const TableDef& tableDef);

    // 删除表
    bool dropTable(const std::string& tableName);

    // 获取表定义
    TableDef getTableDef(const std::string& tableName);

    // 插入记录
    bool insert(const std::string& tableName, const std::vector<std::string>& values);

    // 查询记录
    std::vector<std::vector<std::string>> select(const std::string& tableName,
        const std::string& where = "");

    ~TableManager()
    {
        // 清理所有打开的表
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

    // 将字符串值转换为二进制格式
    bpt::value_t serializeValues(const TableDef& def,
        const std::vector<std::string>& values);

    // 将二进制格式转换回字符串值
    std::vector<std::string> deserializeValues(const TableDef& def,
        const bpt::value_t& data);

    // 保存表定义到文件
    void saveTableDefs();

    // 从文件加载表定义
    void loadTableDefs();
};