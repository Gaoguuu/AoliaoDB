#define _CRT_SECURE_NO_WARNINGS
#include "table_manager.h"
#include <fstream>
#include <sstream>
#include <iostream>
#include <direct.h> // for _mkdir
TableManager::TableManager(const std::string& dbPath) : dbPath(dbPath)
{
    // ȷ��Ŀ¼����
#ifdef _WIN32
    _mkdir(dbPath.c_str());
#else
    mkdir(dbPath.c_str(), 0777);
#endif

    // ���ر���Ԫ����
    loadTableDefs();
}

bool TableManager::createTable(const TableDef& tableDef)
{
    TableDef def = tableDef;
    def.calculateRecordSize(); // ���㲢�����¼��С

    std::string filename = dbPath + def.tableName + ".tbl";
    tables[def.tableName] = new bpt::bplus_tree(filename.c_str(), true);
    tableDefs[def.tableName] = def;

    // �����ṹ��Ԫ�����ļ�
    saveTableDefs();
    return true;
}

bool TableManager::dropTable(const std::string& tableName)
{
    std::cout << "Attempting to drop table: " << tableName << std::endl;

    // �ȼ����Ƿ���ڣ�ʹ�������ı�������ƥ��
    auto it = tables.find(tableName);
    if (it == tables.end())
    {
        // ����Ƿ��Ǵ�Сд�򲿷�ƥ�������
        std::string tableToFind = tableName;
        // �Ƴ�ĩβ�ķֺţ�������ڣ�
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

    // �رղ�ɾ�� B+ ������
    if (it->second)
    {
        // ȷ���ļ����ر�
        it->second->close_tree_file();
        delete it->second;
        it->second = nullptr;
    }
    tables.erase(it);

    // �ӱ���ӳ����ɾ��
    tableDefs.erase(actualTableName);

    // ɾ�����ļ�
    std::string filename = dbPath + actualTableName + ".tbl";
    std::cout << "Deleting file: " << filename << std::endl;

    // ȷ���ļ����رպ���ɾ��
#ifdef _WIN32
    _fcloseall(); // �ر����д򿪵��ļ�
    if (_unlink(filename.c_str()) != 0)
#else
    if (unlink(filename.c_str()) != 0)
#endif
    {
        if (errno != ENOENT) // ���������"�ļ�������"
        {
            std::perror("Failed to delete file");
            std::cerr << "Failed to delete file: " << filename
                << ", error: " << strerror(errno) << std::endl;
            return false;
        }
    }

    // ������º�ı���
    try
    {
        saveTableDefs();

        // ��֤�ļ��Ƿ���ı�ɾ��
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

        // ���л�����
        bpt::value_t value = serializeValues(tableDef, values);
        std::cout << "Serialized data size: " << value.size << std::endl;

        // ʹ�õ�һ���ֶ���Ϊ��
        bpt::key_t key(values[0].c_str());
        std::cout << "Using key: " << key.k << std::endl;

        // ��������
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
        // ���ļ�
        if (!tree->open_tree_file("rb+"))
        {
            std::cerr << "Failed to open table file" << std::endl;
            return results;
        }

        // ��ȡԪ����
        auto meta = tree->get_meta();
        std::cout << "Table meta - leaf_offset: " << meta.leaf_offset
            << ", leaf_node_num: " << meta.leaf_node_num << std::endl;

        // �ӵ�һ��Ҷ�ӽڵ㿪ʼ����
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

            // ������ǰҶ�ӽڵ�����м�¼
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

            // ȷ��4�ֽڶ���
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

                // ��֤д��
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

                // ��֤д��
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
            // ȷ��4�ֽڶ���
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

    // ��д��������
    size_t tableCount = tableDefs.size();
    ofs << tableCount << std::endl;

    for (const auto& pair : tableDefs)
    {
        const auto& def = pair.second;
        // ȷ��ÿ�����嶼���µ�һ�п�ʼ
        ofs << def.tableName << std::endl;
        ofs << def.fields.size() << std::endl;
        for (const auto& field : def.fields)
        {
            ofs << field.name << " "
                << static_cast<int>(field.type) << " "
                << field.size << std::endl;
        }
        ofs << "END_TABLE" << std::endl; // ��ӱ���������
    }

    ofs.flush();
    if (!ofs.good())
    {
        std::cerr << "Error occurred while writing meta file" << std::endl;
    }
    ofs.close();

    // ��֤д�������
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

    // ��ȡ�������
    size_t tableCount;
    if (!(ifs >> tableCount))
    {
        std::cerr << "Failed to read table count" << std::endl;
        return;
    }
    ifs.ignore(); // �������з�

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
        ifs.ignore(); // �������з�

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

        // ��ȡ����������
        std::string endMark;
        std::getline(ifs, endMark);
        std::getline(ifs, endMark);
        if (endMark != "END_TABLE")
        {
            std::cerr << "Invalid table definition format" << std::endl;
            continue;
        }

        // �򿪱��ļ�
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