#define _CRT_SECURE_NO_WARNINGS
#include "table_manager.h"
#include <fstream>
#include <sstream>
#include <iostream>
#include <direct.h> // for _mkdir
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
    def.calculateRecordSize(); // 计算并对齐记录大小

    std::string filename = dbPath + def.tableName + ".tbl";
    tables[def.tableName] = new bpt::bplus_tree(filename.c_str(), true);
    tableDefs[def.tableName] = def;

    // 保存表结构到元数据文件
    saveTableDefs();
    return true;
}

bool TableManager::dropTable(const std::string& tableName)
{
    std::cout << "Attempting to drop table: " << tableName << std::endl;

    // 先检查表是否存在，使用完整的表名进行匹配
    auto it = tables.find(tableName);
    if (it == tables.end())
    {
        // 检查是否是大小写或部分匹配的问题
        std::string tableToFind = tableName;
        // 移除末尾的分号（如果存在）
        if (!tableToFind.empty() && tableToFind.back() == ';')
        {
            tableToFind = tableToFind.substr(0, tableToFind.length() - 1);
        }

        it = tables.find(tableToFind);
        if (it == tables.end())
        {
            std::cerr << "Table not found: " << tableName << std::endl;
            return false;
        }
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

std::vector<std::vector<std::string>> TableManager::select(const std::string& tableName, const std::string& where)
{
    std::vector<std::vector<std::string>> results;
    auto it = tables.find(tableName);
    if (it == tables.end())
    {
        std::cerr << "Table not found: " << tableName << std::endl;
        return results;
    }

    std::cout << "Selecting from table: " << tableName << std::endl;
    bpt::bplus_tree* tree = it->second;

    try
    {
        // 打开文件
        if (!tree->open_tree_file("rb+"))
        {
            std::cerr << "Failed to open table file" << std::endl;
            return results;
        }

        // 获取元数据
        auto meta = tree->get_meta();
        std::cout << "Table meta - leaf_offset: " << meta.leaf_offset
            << ", leaf_node_num: " << meta.leaf_node_num << std::endl;

        // 从第一个叶子节点开始遍历
        bpt::leaf_node_t leaf;
        off_t current = tree->get_first_leaf();

        while (current != 0)
        {
            std::cout << "Reading leaf node at offset: " << current << std::endl;

            if (!tree->read_leaf_node(&leaf, current))
            {
                std::cerr << "Failed to read leaf node at offset: " << current << std::endl;
                break;
            }

            std::cout << "Leaf node contains " << leaf.n << " records" << std::endl;

            // 遍历当前叶子节点的所有记录
            for (size_t i = 0; i < leaf.n; i++)
            {
                const auto& record = leaf.children[i];
                std::cout << "Record " << i << " - Key: " << record.key.k
                    << ", Value size: " << record.value.size << std::endl;

                if (record.value.data && record.value.size > 0)
                {
                    auto row = deserializeValues(tableDefs[tableName], record.value);
                    if (!row.empty())
                    {
                        std::cout << "Deserialized record: ";
                        for (const auto& val : row)
                        {
                            std::cout << val << " ";
                        }
                        std::cout << std::endl;

                        results.push_back(std::move(row));
                    }
                }
            }

            current = leaf.next;
            std::cout << "Next leaf node offset: " << current << std::endl;
        }

        tree->close_tree_file();
    }
    catch (const std::exception& e)
    {
        std::cerr << "Error in select: " << e.what() << std::endl;
        tree->close_tree_file();
    }

    std::cout << "Found " << results.size() << " records" << std::endl;
    return results;
}

bpt::value_t TableManager::serializeValues(const TableDef& def, const std::vector<std::string>& values)
{
    std::cout << "Serializing values..." << std::endl;
    std::cout << "Record size: " << def.recordSize << std::endl;

    bpt::value_t value;
    value.size = def.recordSize;
    value.data = new char[value.size];
    memset(value.data, 0, value.size);

    try
    {
        size_t offset = 0;
        for (size_t i = 0; i < values.size(); i++)
        {
            const auto& field = def.fields[i];
            std::cout << "Serializing field " << field.name << ": " << values[i] << std::endl;

            // 确保4字节对齐
            size_t aligned_offset = (offset + 3) & ~3;
            if (aligned_offset != offset)
            {
                std::cout << "Aligning offset from " << offset << " to " << aligned_offset << std::endl;
                offset = aligned_offset;
            }

            switch (field.type)
            {
            case FieldType::INT:
            {
                int val = std::stoi(values[i]);
                std::cout << "Writing INT " << val << " at offset " << offset << std::endl;
                std::memcpy(value.data + offset, &val, sizeof(int));
                offset += sizeof(int);

                // 验证写入
                int check_val;
                std::memcpy(&check_val, value.data + offset - sizeof(int), sizeof(int));
                std::cout << "Verification read: " << check_val << std::endl;
                break;
            }

            case FieldType::VARCHAR:
            {
                const std::string& str = values[i];
                size_t strLen = std::min(str.length(), field.size - 1);
                std::cout << "Writing VARCHAR '" << str << "' (len=" << strLen << ") at offset " << offset << std::endl;

                std::memcpy(value.data + offset, str.c_str(), strLen);
                value.data[offset + strLen] = '\0';

                // 验证写入
                std::cout << "Verification read: '" << (value.data + offset) << "'" << std::endl;
                offset += field.size;
                break;
            }
            }
        }
        std::cout << "Total bytes written: " << offset << std::endl;
    }
    catch (const std::exception& e)
    {
        std::cerr << "Error during serialization: " << e.what() << std::endl;
        value.clear();
        throw;
    }

    return value;
}

std::vector<std::string> TableManager::deserializeValues(const TableDef& def, const bpt::value_t& data)
{
    std::vector<std::string> values;
    values.reserve(def.fields.size());

    if (!data.data || data.size == 0)
    {
        std::cerr << "Invalid data pointer or size" << std::endl;
        return values;
    }

    std::cout << "Deserializing record of size " << data.size << std::endl;
    try
    {
        size_t offset = 0;
        for (const auto& field : def.fields)
        {
            // 确保4字节对齐
            size_t aligned_offset = (offset + 3) & ~3;
            if (aligned_offset != offset)
            {
                std::cout << "Aligning offset from " << offset << " to " << aligned_offset << std::endl;
                offset = aligned_offset;
            }

            if (offset >= data.size)
            {
                std::cerr << "Offset " << offset << " exceeds data size " << data.size << std::endl;
                break;
            }

            std::cout << "Reading field " << field.name << " at offset " << offset << std::endl;
            switch (field.type)
            {
            case FieldType::INT:
            {
                if (offset + sizeof(int) <= data.size)
                {
                    int val;
                    std::memcpy(&val, data.data + offset, sizeof(int));
                    std::cout << "Read INT value: " << val << std::endl;
                    values.push_back(std::to_string(val));
                    offset += sizeof(int);
                }
                else
                {
                    std::cerr << "Not enough data for INT at offset " << offset << std::endl;
                }
                break;
            }

            case FieldType::VARCHAR:
            {
                if (offset < data.size)
                {
                    const char* str = data.data + offset;
                    size_t maxLen = std::min(field.size - 1, data.size - offset);
                    size_t strLen = strnlen(str, maxLen);
                    std::string value(str, strLen);
                    std::cout << "Read VARCHAR value: '" << value << "' (len=" << strLen << ")" << std::endl;
                    values.push_back(std::move(value));
                    offset += field.size;
                }
                else
                {
                    std::cerr << "Not enough data for VARCHAR at offset " << offset << std::endl;
                }
                break;
            }
            }
        }
    }
    catch (const std::exception& e)
    {
        std::cerr << "Error during deserialization: " << e.what() << std::endl;
        values.clear();
    }

    return values;
}

void TableManager::saveTableDefs()
{
    std::string metaFile = dbPath + "tables.meta";
    std::ofstream ofs(metaFile);
    if (!ofs.is_open())
    {
        std::cerr << "Failed to open meta file for writing: " << metaFile << std::endl;
        return;
    }

    // 先写入表的数量
    size_t tableCount = tableDefs.size();
    ofs << tableCount << std::endl;

    for (const auto& pair : tableDefs)
    {
        const auto& def = pair.second;
        // 确保每个表定义都在新的一行开始
        ofs << def.tableName << std::endl;
        ofs << def.fields.size() << std::endl;
        for (const auto& field : def.fields)
        {
            ofs << field.name << " "
                << static_cast<int>(field.type) << " "
                << field.size << std::endl;
        }
        ofs << "END_TABLE" << std::endl; // 添加表定义结束标记
    }

    ofs.flush();
    if (!ofs.good())
    {
        std::cerr << "Error occurred while writing meta file" << std::endl;
    }
    ofs.close();

    // 验证写入的内容
    std::cout << "Saved " << tableCount << " tables to meta file" << std::endl;
}

void TableManager::loadTableDefs()
{
    std::string metaFile = dbPath + "tables.meta";
    std::ifstream ifs(metaFile);
    if (!ifs.is_open())
    {
        std::cerr << "Failed to open meta file: " << metaFile << std::endl;
        return;
    }

    // 读取表的数量
    size_t tableCount;
    if (!(ifs >> tableCount))
    {
        std::cerr << "Failed to read table count" << std::endl;
        return;
    }
    ifs.ignore(); // 跳过换行符

    std::cout << "Loading " << tableCount << " tables from meta file" << std::endl;

    for (size_t t = 0; t < tableCount && ifs.good(); ++t)
    {
        std::string tableName;
        if (!std::getline(ifs, tableName) || tableName.empty())
        {
            std::cerr << "Failed to read table name" << std::endl;
            break;
        }

        std::cout << "Loading table: " << tableName << std::endl;

        TableDef def;
        def.tableName = tableName;

        size_t fieldCount;
        if (!(ifs >> fieldCount))
        {
            std::cerr << "Failed to read field count for table: " << tableName << std::endl;
            break;
        }
        ifs.ignore(); // 跳过换行符

        std::cout << "Field count: " << fieldCount << std::endl;

        size_t totalSize = 0;
        for (size_t i = 0; i < fieldCount && ifs.good(); i++)
        {
            FieldDef field;
            int type;
            if (ifs >> field.name >> type >> field.size)
            {
                field.type = static_cast<FieldType>(type);
                def.fields.push_back(field);

                switch (field.type)
                {
                case FieldType::INT:
                    totalSize += sizeof(int);
                    break;
                case FieldType::VARCHAR:
                    totalSize += field.size;
                    break;
                }
                std::cout << "Loaded field: " << field.name << std::endl;
            }
            else
            {
                std::cerr << "Failed to read field " << i << " for table: " << tableName << std::endl;
            }
        }
        def.recordSize = totalSize;

        // 读取表定义结束标记
        std::string endMark;
        std::getline(ifs, endMark);
        std::getline(ifs, endMark);
        if (endMark != "END_TABLE")
        {
            std::cerr << "Invalid table definition format" << std::endl;
            continue;
        }

        // 打开表文件
        std::string filename = dbPath + tableName + ".tbl";
        try
        {
            auto* tree = new bpt::bplus_tree(filename.c_str());
            if (tree && tree->get_meta().order == BP_ORDER)
            {
                tables[tableName] = tree;
                tableDefs[tableName] = def;
                std::cout << "Successfully loaded table: " << tableName << std::endl;
            }
            else
            {
                std::cerr << "Invalid table file format: " << filename << std::endl;
                delete tree;
            }
        }
        catch (const std::exception& e)
        {
            std::cerr << "Error loading table " << tableName << ": " << e.what() << std::endl;
        }
    }

    ifs.close();
    std::cout << "Loaded " << tables.size() << " tables" << std::endl;
}