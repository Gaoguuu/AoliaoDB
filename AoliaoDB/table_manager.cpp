#define _CRT_SECURE_NO_WARNINGS
#include "table_manager.h"
#include <fstream>
#include <sstream>
#include <iostream>
#include <direct.h> // for _mkdir

TableManager::~TableManager()
{
    // 清理所有打开的表
    for (auto& pair : tables)
    {
        if (pair.second)
        {
            pair.second->close_tree_file(); // 确保文件被关闭
            delete pair.second;
            pair.second = nullptr;
        }
    }
    tables.clear();
    tableDefs.clear();
}

TableManager::TableManager(const std::string& dbPath) : dbPath(dbPath)
{
    // 确保目录存在
#ifdef _WIN32
    _mkdir(dbPath.c_str());
#else
    mkdir(dbPath.c_str(), 0777);
#endif

    // 加载表定义元数据
    loadTableDefs();
}

bool TableManager::createTable(const TableDef& tableDef)
{
    TableDef def = tableDef;
    size_t totalSize = def.calculateRecordSize();

    std::string filename = dbPath + def.tableName + ".tbl";
    tables[def.tableName] = new bpt::bplus_tree(filename.c_str(), true);
    tableDefs[def.tableName] = def;

    // 保存表结构到元数据文件
    saveTableDefs();
    return true;
}

bool TableManager::dropTable(const std::string& tableName)
{
    // 清理表名（移除空格和分号）
    std::string cleanTableName = tableName;
    cleanTableName = cleanTableName.substr(0, cleanTableName.find_last_not_of(" ;") + 1);
    cleanTableName = cleanTableName.substr(cleanTableName.find_first_not_of(" "));

    std::cout << "Attempting to drop table: " << cleanTableName << std::endl;

    auto it = tables.find(cleanTableName);
    if (it == tables.end())
    {
        std::cerr << "Table not found: " << cleanTableName << std::endl;
        return false;
    }

    std::string actualTableName = it->first;
    std::cout << "Found table: " << actualTableName << std::endl;

    // 关闭并删除 B+ 树对象
    if (it->second)
    {
        // 确保文件被关闭
        it->second->close_tree_file();
        delete it->second;
        it->second = nullptr;
    }
    tables.erase(it);

    // 从表定义映射中删除
    tableDefs.erase(actualTableName);

    // 删除表文件
    std::string filename = dbPath + actualTableName + ".tbl";
    std::cout << "Deleting file: " << filename << std::endl;

    // 确保文件被关闭后再删除
#ifdef _WIN32
    _fcloseall(); // 关闭所有打开的文件
    if (_unlink(filename.c_str()) != 0)
#else
    if (unlink(filename.c_str()) != 0)
#endif
    {
        if (errno != ENOENT) // 如果错误不是"文件不存在"
        {
            std::perror("Failed to delete file");
            std::cerr << "Failed to delete file: " << filename
                << ", error: " << strerror(errno) << std::endl;
            return false;
        }
    }

    // 保存更新后的表定义
    try
    {
        saveTableDefs();

        // 验证文件是否真的被删除
        std::ifstream check(filename.c_str());
        if (check.good())
        {
            std::cerr << "Warning: File still exists after deletion" << std::endl;
            check.close();
            return false;
        }

        std::cout << "Table " << actualTableName << " dropped successfully" << std::endl;
        return true;
    }
    catch (const std::exception& e)
    {
        std::cerr << "Error saving table definitions after drop: " << e.what() << std::endl;
        return false;
    }
}

TableDef TableManager::getTableDef(const std::string& tableName)
{
    auto it = tableDefs.find(tableName);
    if (it != tableDefs.end())
    {
        return it->second;
    }
    return TableDef();
}

bool TableManager::insert(const std::string& tableName, const std::vector<std::string>& values)
{
    auto it = tables.find(tableName);
    if (it == tables.end())
    {
        std::cerr << "Table not found: " << tableName << std::endl;
        return false;
    }

    auto tableDefIt = tableDefs.find(tableName);
    if (tableDefIt == tableDefs.end())
    {
        std::cerr << "Table definition not found: " << tableName << std::endl;
        return false;
    }

    auto& tableDef = tableDefIt->second;
    if (values.size() != tableDef.fields.size())
    {
        std::cerr << "Field count mismatch. Expected: " << tableDef.fields.size()
            << ", Got: " << values.size() << std::endl;
        return false;
    }

    try
    {
        std::cout << "Inserting into table: " << tableName << std::endl;
        std::cout << "Values: ";
        for (const auto& val : values)
        {
            std::cout << val << " ";
        }
        std::cout << std::endl;

        // 序列化数据
        bpt::value_t value = serializeValues(tableDef, values);
        std::cout << "Serialized data size: " << value.size << std::endl;

        // 使用第一个字段作为键
        bpt::key_t key(values[0].c_str());
        std::cout << "Using key: " << key.k << std::endl;

        // 插入数据
        int result = it->second->insert(key, value);
        std::cout << "Insert result code: " << result << std::endl;

        if (result != 0)
        {
            std::cerr << "Failed to insert into B+ tree, error code: " << result << std::endl;
            return false;
        }
        return true;
    }
    catch (const std::exception& e)
    {
        std::cerr << "Error during insert: " << e.what() << std::endl;
        return false;
    }
}

// 解析 WHERE 子句
std::vector<TableManager::Condition> TableManager::parseWhereClause(const std::string& where)
{
    std::vector<Condition> conditions;
    if (where.empty())
        return conditions;

    std::istringstream iss(where);
    std::string field, op, value;

    // 支持多个条件（用 AND 连接）
    while (iss >> field)
    {
        if (!(iss >> op >> value))
            break;

        // 清理值中的引号
        if (value.front() == '"' || value.front() == '\'')
            value = value.substr(1, value.length() - 2);

        conditions.push_back({ field, op, value });

        // 检查是否还有 AND
        std::string next;
        if (iss >> next && next != "AND")
            break;
    }

    return conditions;
}

// 检查记录是否满足条件
bool TableManager::matchConditions(
    const std::vector<std::string>& record,
    const TableDef& def,
    const std::vector<Condition>& conditions)
{
    // 如果没有条件，返回 true
    if (conditions.empty())
        return true;

    // 所有条件都要满足（AND 逻辑）
    for (const auto& cond : conditions)
    {
        int idx = getFieldIndex(def, cond.field);
        if (idx == -1 || static_cast<size_t>(idx) >= record.size())
            return false;

        const std::string& value = record[idx];
        bool match = false; // 用于标记当前条件是否匹配

        // 根据字段类型进行比较
        if (def.fields[idx].type == FieldType::INT)
        {
            int recordVal = std::stoi(value);
            int condVal = std::stoi(cond.value);

            if (cond.op == "=")
                match = (recordVal == condVal);
            if (cond.op == ">")
                match = (recordVal > condVal);
            if (cond.op == "<")
                match = (recordVal < condVal);
            if (cond.op == ">=")
                match = (recordVal >= condVal);
            if (cond.op == "<=")
                match = (recordVal <= condVal);
            if (cond.op == "!=")
                match = (recordVal != condVal);
        }
        else
        {
            if (cond.op == "=")
                match = (value == cond.value);
            if (cond.op == ">")
                match = (value > cond.value);
            if (cond.op == "<")
                match = (value < cond.value);
            if (cond.op == ">=")
                match = (value >= cond.value);
            if (cond.op == "<=")
                match = (value <= cond.value);
            if (cond.op == "!=")
                match = (value != cond.value);
        }

        // 如果当前条件不匹配，直接返回 false
        if (!match)
            return false;
    }

    // 所有条件都匹配
    return true;
}

// 获取字段在记录中的位置
int TableManager::getFieldIndex(const TableDef& def, const std::string& fieldName)
{
    for (int i = 0; i < static_cast<int>(def.fields.size()); i++)
    {
        if (def.fields[i].name == fieldName)
            return i;
    }
    return -1;
}

// 单表查询实现
std::vector<std::vector<std::string>> TableManager::select(
    const std::string& tableName,
    const std::vector<std::string>& fields,
    const std::string& where)
{
    std::vector<std::vector<std::string>> results;
    auto it = tables.find(tableName);
    if (it == tables.end() || !it->second)
    {
        std::cerr << "Table not found or invalid: " << tableName << std::endl;
        return results;
    }

    TableDef def = tableDefs[tableName];
    auto conditions = parseWhereClause(where);

    // 确定要返回的字段索引
    std::vector<int> fieldIndices;
    if (fields.empty())
    {
        // 返回所有字段
        for (int i = 0; i < static_cast<int>(def.fields.size()); i++)
            fieldIndices.push_back(i);
    }
    else
    {
        // 返回指定字段
        for (const auto& field : fields)
        {
            int idx = getFieldIndex(def, field);
            if (idx != -1)
                fieldIndices.push_back(idx);
        }
    }

    // 确保文件是打开的
    if (!it->second->open_tree_file())
    {
        std::cerr << "Failed to open table file" << std::endl;
        return results;
    }

    try
    {
        // 遍历 B+ 树获取所有记录
        auto records = it->second->traverse();
        std::cout << "Found " << records.size() << " records in table" << std::endl;

        // 处理每条记录
        for (const auto& record : records)
        {
            auto values = deserializeValues(def, record.value);
            if (matchConditions(values, def, conditions))
            {
                std::vector<std::string> row;
                for (int idx : fieldIndices)
                {
                    if (idx >= 0 && idx < static_cast<int>(values.size()))
                    {
                        row.push_back(values[idx]);
                    }
                }
                if (!row.empty())
                {
                    results.push_back(row);
                }
            }
        }
    }
    catch (const std::exception& e)
    {
        std::cerr << "Error during select: " << e.what() << std::endl;
    }

    // 关闭文件
    it->second->close_tree_file();

    return results;
}

bpt::value_t TableManager::serializeValues(const TableDef& def, const std::vector<std::string>& values)
{
    size_t totalSize = def.calculateRecordSize();
    bpt::value_t result;
    result.size = totalSize;
    result.data = new char[totalSize];
    memset(result.data, 0, totalSize);

    size_t offset = 0;
    for (size_t i = 0; i < def.fields.size(); i++)
    {
        const auto& field = def.fields[i];
        std::string value = values[i];

        // 移除引号（如果存在）
        if (value.front() == '\'' || value.front() == '"')
            value = value.substr(1, value.length() - 2);

        // 4字节对齐
        offset = (offset + 3) & ~3;

        switch (field.type)
        {
        case FieldType::INT:
        {
            int val = std::stoi(value);
            memcpy(result.data + offset, &val, sizeof(int));
            offset += sizeof(int);
            break;
        }
        case FieldType::VARCHAR:
        {
            size_t strLen = std::min(value.length(), field.size - 1);
            memcpy(result.data + offset, value.c_str(), strLen);
            result.data[offset + strLen] = '\0';
            offset += field.size;
            break;
        }
        case FieldType::DATETIME:
        {
            // 确保日期时间格式正确
            if (value.length() >= 19) // "YYYY-MM-DD HH:MM:SS"
            {
                memcpy(result.data + offset, value.c_str(), 19);
                result.data[offset + 19] = '\0';
            }
            offset += sizeof(time_t);
            break;
        }
        case FieldType::BOOL:
        {
            bool val = (value == "true" || value == "1");
            memcpy(result.data + offset, &val, sizeof(bool));
            offset += sizeof(bool);
            break;
        }
        case FieldType::FLOAT:
        {
            float val = std::stof(value);
            memcpy(result.data + offset, &val, sizeof(float));
            offset += sizeof(float);
            break;
        }
        case FieldType::DOUBLE:
        {
            double val = std::stod(value);
            memcpy(result.data + offset, &val, sizeof(double));
            offset += sizeof(double);
            break;
        }
        }
    }

    return result;
}

std::vector<std::string> TableManager::deserializeValues(const TableDef& def, const bpt::value_t& data)
{
    std::vector<std::string> result;
    result.reserve(def.fields.size());

    size_t offset = 0;
    for (const auto& field : def.fields)
    {
        // 4字节对齐
        offset = (offset + 3) & ~3;

        try
        {
            switch (field.type)
            {
            case FieldType::INT:
            {
                int val;
                memcpy(&val, data.data + offset, sizeof(int));
                result.push_back(std::to_string(val));
                offset += sizeof(int);
                break;
            }
            case FieldType::VARCHAR:
            {
                std::string value(data.data + offset);
                // 添加引号
                result.push_back("'" + value + "'");
                offset += field.size;
                break;
            }
            case FieldType::DATETIME:
            {
                std::string value(data.data + offset);
                // 添加引号
                result.push_back("'" + value + "'");
                offset += sizeof(time_t);
                break;
            }
            case FieldType::BOOL:
            {
                bool val;
                memcpy(&val, data.data + offset, sizeof(bool));
                result.push_back(val ? "true" : "false");
                offset += sizeof(bool);
                break;
            }
            case FieldType::FLOAT:
            {
                float val;
                memcpy(&val, data.data + offset, sizeof(float));
                result.push_back(std::to_string(val));
                offset += sizeof(float);
                break;
            }
            case FieldType::DOUBLE:
            {
                double val;
                memcpy(&val, data.data + offset, sizeof(double));
                result.push_back(std::to_string(val));
                offset += sizeof(double);
                break;
            }
            }
        }
        catch (const std::exception& e)
        {
            std::cerr << "Error deserializing field " << field.name << ": " << e.what() << std::endl;
            result.push_back("");
        }
    }

    return result;
}

void TableManager::saveTableDefs()
{
    std::ofstream ofs(dbPath + "tables.meta");
    if (!ofs)
    {
        std::cerr << "Failed to open metadata file for writing" << std::endl;
        return;
    }

    // 写入表的数量
    ofs << tableDefs.size() << std::endl;

    // 写入每个表的定义
    for (const auto& pair : tableDefs)
    {
        const auto& def = pair.second;

        // 写入表名
        ofs << def.tableName << std::endl;

        // 写入字段数量
        ofs << def.fields.size() << std::endl;

        // 写入每个字段的定义
        for (const auto& field : def.fields)
        {
            ofs << field.name << " "
                << static_cast<int>(field.type) << " "
                << field.size << " "
                << field.is_key << " "
                << field.is_nullable << " "
                << field.is_unique << " "
                << field.is_index << " "
                << field.default_value << std::endl;
        }

        // 写入表注释
        ofs << def.comment << std::endl;

        // 写入创建时间和更新时间
        ofs << def.create_time << std::endl;
        ofs << def.update_time << std::endl;

        // 写入表定义结束标记
        ofs << "END_TABLE" << std::endl;
    }
}

void TableManager::loadTableDefs()
{
    std::ifstream ifs(dbPath + "tables.meta");
    if (!ifs)
    {
        std::cout << "No existing tables found" << std::endl;
        return;
    }

    std::string line;
    // 读取表的数量
    std::getline(ifs, line);
    size_t tableCount = std::stoi(line);

    for (size_t i = 0; i < tableCount; i++)
    {
        // 读取表名
        std::string tableName;
        std::getline(ifs, tableName);
        if (tableName.empty())
            continue;

        TableDef def;
        def.tableName = tableName;

        // 读取字段数量
        std::getline(ifs, line);
        size_t fieldCount = std::stoi(line);

        // 读取每个字段的定义
        for (size_t j = 0; j < fieldCount; j++)
        {
            std::getline(ifs, line);
            std::istringstream iss(line);

            FieldDef field;
            int type;

            // 读取字段名和类型信息
            if (!(iss >> field.name >> type >> field.size >>
                field.is_key >> field.is_nullable >>
                field.is_unique >> field.is_index))
            {
                std::cerr << "Failed to read field properties for field " << j
                    << " in table " << tableName << std::endl;
                continue;
            }

            // 读取默认值（剩余的所有内容）
            std::string remaining;
            std::getline(iss >> std::ws, remaining);
            field.default_value = remaining;

            field.type = static_cast<FieldType>(type);
            def.fields.push_back(field);

            std::cout << "Loaded field: " << field.name
                << " (type=" << type
                << ", size=" << field.size
                << ", default=" << field.default_value << ")" << std::endl;
        }

        // 读取表注释
        std::getline(ifs, def.comment);

        // 读取创建时间和更新时间
        std::getline(ifs, def.create_time);
        std::getline(ifs, def.update_time);

        // 读取表定义结束标记
        std::string endMark;
        std::getline(ifs, endMark);
        if (endMark != "END_TABLE")
        {
            std::cerr << "Invalid table definition format for table: " << tableName << std::endl;
            continue;
        }

        // 打开表文件
        std::string filename = dbPath + tableName + ".tbl";
        try
        {
            auto* tree = new bpt::bplus_tree(filename.c_str());
            tables[tableName] = tree;
            tableDefs[tableName] = def;
            std::cout << "Successfully loaded table: " << tableName << std::endl;
        }
        catch (const std::exception& e)
        {
            std::cerr << "Error loading table " << tableName << ": " << e.what() << std::endl;
        }
    }

    std::cout << "Loaded " << tables.size() << " tables" << std::endl;
}

bool TableManager::addField(const std::string& tableName, const std::string& fieldName,
    FieldType type, size_t size)
{
    // 清理表名
    std::string cleanTableName = tableName;
    cleanTableName = cleanTableName.substr(0, cleanTableName.find_last_not_of(" ;") + 1);
    cleanTableName = cleanTableName.substr(cleanTableName.find_first_not_of(" "));

    auto it = tableDefs.find(cleanTableName);
    if (it == tableDefs.end())
    {
        std::cerr << "Table not found: " << cleanTableName << std::endl;
        return false;
    }

    TableDef oldDef = it->second;
    TableDef newDef = oldDef;

    // 添加新字段
    if (!newDef.addField(fieldName, type, size))
    {
        std::cerr << "Failed to add field: " << fieldName << std::endl;
        return false;
    }

    // 创建临时表并迁移数据
    if (!rebuildTable(cleanTableName, oldDef, newDef))
    {
        std::cerr << "Failed to rebuild table" << std::endl;
        return false;
    }

    // 更新表定义
    tableDefs[cleanTableName] = newDef;
    saveTableDefs();
    return true;
}

bool TableManager::dropField(const std::string& tableName, const std::string& fieldName)
{
    auto it = tableDefs.find(tableName);
    if (it == tableDefs.end())
    {
        std::cerr << "Table not found: " << tableName << std::endl;
        return false;
    }

    TableDef oldDef = it->second;
    TableDef newDef = oldDef;

    if (!newDef.removeField(fieldName))
    {
        std::cerr << "Failed to remove field: " << fieldName << std::endl;
        return false;
    }

    if (!rebuildTable(tableName, oldDef, newDef))
    {
        std::cerr << "Failed to rebuild table" << std::endl;
        return false;
    }

    tableDefs[tableName] = newDef;
    saveTableDefs();
    return true;
}

bool TableManager::renameField(const std::string& tableName,
    const std::string& oldName,
    const std::string& newName)
{
    auto it = tableDefs.find(tableName);
    if (it == tableDefs.end())
    {
        std::cerr << "Table not found: " << tableName << std::endl;
        return false;
    }

    TableDef oldDef = it->second;
    TableDef newDef = oldDef;

    if (!newDef.renameField(oldName, newName))
    {
        std::cerr << "Failed to rename field: " << oldName << " to " << newName << std::endl;
        return false;
    }

    // 创建临时表并迁移数据
    if (!rebuildTable(tableName, oldDef, newDef, oldName, newName))
    {
        std::cerr << "Failed to rebuild table" << std::endl;
        return false;
    }

    // 更新表定义
    tableDefs[tableName] = newDef;
    saveTableDefs();
    return true;
}

bool TableManager::alterFieldType(const std::string& tableName,
    const std::string& fieldName,
    FieldType newType, size_t newSize)
{
    // 清理表名
    std::string cleanTableName = tableName;
    cleanTableName = cleanTableName.substr(0, cleanTableName.find_last_not_of(" ;") + 1);
    cleanTableName = cleanTableName.substr(cleanTableName.find_first_not_of(" "));

    auto it = tableDefs.find(cleanTableName);
    if (it == tableDefs.end())
    {
        std::cerr << "Table not found: " << cleanTableName << std::endl;
        return false;
    }

    TableDef oldDef = it->second;
    TableDef newDef = oldDef;

    if (!newDef.alterFieldType(fieldName, newType, newSize))
    {
        std::cerr << "Failed to alter field type: " << fieldName << std::endl;
        return false;
    }

    // 创建临时表并迁移数据
    if (!rebuildTable(cleanTableName, oldDef, newDef))
    {
        std::cerr << "Failed to rebuild table" << std::endl;
        return false;
    }

    // 更新表定义
    tableDefs[cleanTableName] = newDef;
    saveTableDefs();
    return true;
}

bool TableManager::rebuildTable(const std::string& tableName,
    const TableDef& oldDef,
    const TableDef& newDef,
    const std::string& oldFieldName,
    const std::string& newFieldName)
{
    // 创建临时表
    std::string tempTableName = tableName + "_temp";
    TableDef tempDef = newDef;
    tempDef.tableName = tempTableName;

    // 创建临时表
    if (!createTable(tempDef))
    {
        std::cerr << "Failed to create temporary table" << std::endl;
        return false;
    }

    // 读取原表数据
    auto records = select(tableName);
    std::cout << "Read " << records.size() << " records from original table" << std::endl;

    // 转换并插入数据到新表
    for (const auto& record : records)
    {
        std::vector<std::string> newRecord(tempDef.fields.size());

        // 初始化新记录的所有字段为默认值
        for (size_t j = 0; j < tempDef.fields.size(); j++)
        {
            const auto& newField = tempDef.fields[j];
            newRecord[j] = newField.default_value.empty() ? "NULL" : newField.default_value;
        }

        // 映射旧字段到新字段
        for (size_t i = 0; i < oldDef.fields.size(); i++)
        {
            const auto& oldField = oldDef.fields[i];

            // 在新表定义中查找对应字段
            for (size_t j = 0; j < tempDef.fields.size(); j++)
            {
                const auto& newField = tempDef.fields[j];

                // 检查字段名是否匹配（考虑重命名的情况）
                if (newField.name == oldField.name ||
                    (!oldFieldName.empty() && oldField.name == oldFieldName &&
                        newField.name == newFieldName))
                {
                    // 保持原始值的格式（包括引号）
                    newRecord[j] = record[i];
                    break;
                }
            }
        }

        // 打印调试信息
        std::cout << "Inserting into table: " << tempTableName << std::endl;
        std::cout << "Values: ";
        for (const auto& val : newRecord)
        {
            std::cout << val << " ";
        }
        std::cout << std::endl;

        // 插入数据到临时表
        if (!insert(tempTableName, newRecord))
        {
            std::cerr << "Failed to insert record into temporary table" << std::endl;
            dropTable(tempTableName);
            return false;
        }
    }

    // 关闭原表和临时表
    if (tables[tableName])
        tables[tableName]->close_tree_file();
    if (tables[tempTableName])
        tables[tempTableName]->close_tree_file();

    // 删除原表文件
    std::string oldFileName = dbPath + tableName + ".tbl";
    std::string tempFileName = dbPath + tempTableName + ".tbl";

#ifdef _WIN32
    _unlink(oldFileName.c_str());
#else
    unlink(oldFileName.c_str());
#endif

    // 重命名临时表文件
    if (rename(tempFileName.c_str(), oldFileName.c_str()) != 0)
    {
        std::cerr << "Failed to rename temporary table file" << std::endl;
        return false;
    }

    // 更新表定义
    auto* oldTable = tables[tableName];
    auto* tempTable = tables[tempTableName];

    delete oldTable;
    delete tempTable;

    tables.erase(tempTableName);
    tables[tableName] = new bpt::bplus_tree(oldFileName.c_str());
    tableDefs[tableName] = newDef;
    tableDefs.erase(tempTableName);

    return true;
}

bool TableManager::renameTable(const std::string& oldName, const std::string& newName)
{
    auto it = tableDefs.find(oldName);
    if (it == tableDefs.end())
    {
        std::cerr << "Table not found: " << oldName << std::endl;
        return false;
    }

    if (tableDefs.find(newName) != tableDefs.end())
    {
        std::cerr << "Table already exists: " << newName << std::endl;
        return false;
    }

    // 关闭表文件
    if (tables[oldName])
        tables[oldName]->close_tree_file();

    // 重命名文件
    std::string oldFileName = dbPath + oldName + ".tbl";
    std::string newFileName = dbPath + newName + ".tbl";
    if (rename(oldFileName.c_str(), newFileName.c_str()) != 0)
    {
        std::cerr << "Failed to rename table file" << std::endl;
        return false;
    }

    // 更新内存中的表定义
    TableDef def = it->second;
    def.tableName = newName;
    tableDefs[newName] = def;
    tableDefs.erase(oldName);

    // 更新B+树对象
    auto* oldTree = tables[oldName];
    tables.erase(oldName);
    tables[newName] = new bpt::bplus_tree(newFileName.c_str());
    delete oldTree;

    saveTableDefs();
    return true;
}

bool TableManager::deleteRecords(const std::string& tableName, const std::string& where)
{
    // 清理表名
    std::string cleanTableName = tableName;
    cleanTableName = cleanTableName.substr(0, cleanTableName.find_last_not_of(" ;") + 1);
    cleanTableName = cleanTableName.substr(cleanTableName.find_first_not_of(" "));

    auto it = tables.find(cleanTableName);
    if (it == tables.end())
    {
        std::cerr << "Table not found: " << cleanTableName << std::endl;
        return false;
    }

    auto tableDefIt = tableDefs.find(cleanTableName);
    if (tableDefIt == tableDefs.end())
    {
        std::cerr << "Table definition not found: " << cleanTableName << std::endl;
        return false;
    }

    // 解析WHERE条件
    auto conditions = parseWhereClause(where);

    // 获取所有匹配的记录
    auto records = select(cleanTableName, std::vector<std::string>(), where);
    std::cout << "Found " << records.size() << " records to delete" << std::endl;

    // 删除匹配的记录
    for (const auto& record : records)
    {
        bpt::key_t key(record[0].c_str());
        if (it->second->remove(key) != 0)
        {
            std::cerr << "Failed to delete record with key: " << record[0] << std::endl;
            return false;
        }
    }

    return true;
}

bool TableManager::update(const std::string& tableName,
    const std::vector<std::pair<std::string, std::string>>& setValues,
    const std::string& where)
{
    // 清理表名（移除空格和分号）
    std::string cleanTableName = tableName;
    cleanTableName = cleanTableName.substr(0, cleanTableName.find_last_not_of(" ;") + 1);
    cleanTableName = cleanTableName.substr(cleanTableName.find_first_not_of(" "));

    auto it = tables.find(cleanTableName);
    if (it == tables.end())
    {
        std::cerr << "Table not found: " << cleanTableName << std::endl;
        return false;
    }

    auto tableDefIt = tableDefs.find(cleanTableName);
    if (tableDefIt == tableDefs.end())
    {
        std::cerr << "Table definition not found: " << cleanTableName << std::endl;
        return false;
    }

    // 获取要更新的记录
    auto records = select(cleanTableName, std::vector<std::string>(), where);
    std::cout << "Found " << records.size() << " records to update" << std::endl;

    // 更新每条记录
    for (const auto& record : records)
    {
        std::vector<std::string> newRecord = record;

        // 应用更新
        for (const auto& setValue : setValues)
        {
            int fieldIndex = getFieldIndex(tableDefIt->second, setValue.first);
            if (fieldIndex >= 0)
            {
                // 处理值中的引号
                std::string value = setValue.second;
                if (value.front() == '\'' && value.back() == '\'')
                {
                    value = value.substr(1, value.length() - 2);
                }
                newRecord[fieldIndex] = value;
            }
        }

        // 删除旧记录
        bpt::key_t oldKey(record[0].c_str());
        it->second->remove(oldKey);

        // 插入新记录
        if (!insert(cleanTableName, newRecord))
        {
            std::cerr << "Failed to update record with key: " << record[0] << std::endl;
            return false;
        }
    }

    return true;
}

std::vector<std::vector<std::string>> TableManager::selectJoin(
    const std::string& table1,
    const std::string& table2,
    const std::string& joinCondition,
    const std::vector<std::string>& fields,
    const std::string& where)
{
    std::vector<std::vector<std::string>> results;

    // 获取两个表的定义
    auto def1 = tableDefs.find(table1);
    auto def2 = tableDefs.find(table2);
    if (def1 == tableDefs.end() || def2 == tableDefs.end())
    {
        std::cerr << "Table not found" << std::endl;
        return results;
    }

    // 解析连接条件
    JoinCondition jc = parseJoinCondition(joinCondition);
    if (jc.table1Field.empty() || jc.table2Field.empty())
    {
        std::cerr << "Invalid join condition" << std::endl;
        return results;
    }

    // 获取两个表的数据
    auto records1 = select(table1);
    auto records2 = select(table2);

    // 获取连接字段的索引
    int joinIndex1 = getFieldIndex(def1->second, jc.table1Field);
    int joinIndex2 = getFieldIndex(def2->second, jc.table2Field);

    if (joinIndex1 < 0 || joinIndex2 < 0)
    {
        std::cerr << "Join field not found" << std::endl;
        return results;
    }

    // 执行连接操作
    for (const auto& record1 : records1)
    {
        for (const auto& record2 : records2)
        {
            // 检查连接条件
            if (record1[joinIndex1] == record2[joinIndex2])
            {
                std::vector<std::string> joinedRecord;

                // 如果指定了字段，只选择指定的字段
                if (!fields.empty())
                {
                    for (const auto& field : fields)
                    {
                        // 检查字段属于哪个表
                        size_t dotPos = field.find('.');
                        if (dotPos != std::string::npos)
                        {
                            std::string tableName = field.substr(0, dotPos);
                            std::string fieldName = field.substr(dotPos + 1);

                            if (tableName == table1)
                            {
                                int idx = getFieldIndex(def1->second, fieldName);
                                if (idx >= 0)
                                    joinedRecord.push_back(record1[idx]);
                            }
                            else if (tableName == table2)
                            {
                                int idx = getFieldIndex(def2->second, fieldName);
                                if (idx >= 0)
                                    joinedRecord.push_back(record2[idx]);
                            }
                        }
                    }
                }
                else
                {
                    // 如果没有指定字段，合并所有字段
                    joinedRecord.insert(joinedRecord.end(), record1.begin(), record1.end());
                    joinedRecord.insert(joinedRecord.end(), record2.begin(), record2.end());
                }

                results.push_back(joinedRecord);
            }
        }
    }

    return results;
}

TableManager::JoinCondition TableManager::parseJoinCondition(const std::string& condition)
{
    JoinCondition jc;

    // 期望格式: table1.field1 = table2.field2
    size_t equalPos = condition.find('=');
    if (equalPos == std::string::npos)
        return jc;

    std::string left = condition.substr(0, equalPos);
    std::string right = condition.substr(equalPos + 1);

    // 清理空格
    left = left.substr(left.find_first_not_of(" \t"));
    left = left.substr(0, left.find_last_not_of(" \t") + 1);
    right = right.substr(right.find_first_not_of(" \t"));
    right = right.substr(0, right.find_last_not_of(" \t") + 1);

    // 解析字段名
    size_t dotPos = left.find('.');
    if (dotPos != std::string::npos)
        jc.table1Field = left.substr(dotPos + 1);

    dotPos = right.find('.');
    if (dotPos != std::string::npos)
        jc.table2Field = right.substr(dotPos + 1);

    return jc;
}