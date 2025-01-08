#define _CRT_SECURE_NO_WARNINGS
#include "table_manager.h"
#include <fstream>
#include <sstream>
#include <iostream>
#include <direct.h> // for _mkdir

TableManager::~TableManager()
{
    // �������д򿪵ı�
    for (auto& pair : tables)
    {
        if (pair.second)
        {
            pair.second->close_tree_file(); // ȷ���ļ����ر�
            delete pair.second;
            pair.second = nullptr;
        }
    }
    tables.clear();
    tableDefs.clear();
}

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
    size_t totalSize = def.calculateRecordSize();

    std::string filename = dbPath + def.tableName + ".tbl";
    tables[def.tableName] = new bpt::bplus_tree(filename.c_str(), true);
    tableDefs[def.tableName] = def;

    // �����ṹ��Ԫ�����ļ�
    saveTableDefs();
    return true;
}

bool TableManager::dropTable(const std::string& tableName)
{
    // ����������Ƴ��ո�ͷֺţ�
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

// ���� WHERE �Ӿ�
std::vector<TableManager::Condition> TableManager::parseWhereClause(const std::string& where)
{
    std::vector<Condition> conditions;
    if (where.empty())
        return conditions;

    std::istringstream iss(where);
    std::string field, op, value;

    // ֧�ֶ���������� AND ���ӣ�
    while (iss >> field)
    {
        if (!(iss >> op >> value))
            break;

        // ����ֵ�е�����
        if (value.front() == '"' || value.front() == '\'')
            value = value.substr(1, value.length() - 2);

        conditions.push_back({ field, op, value });

        // ����Ƿ��� AND
        std::string next;
        if (iss >> next && next != "AND")
            break;
    }

    return conditions;
}

// ����¼�Ƿ���������
bool TableManager::matchConditions(
    const std::vector<std::string>& record,
    const TableDef& def,
    const std::vector<Condition>& conditions)
{
    // ���û������������ true
    if (conditions.empty())
        return true;

    // ����������Ҫ���㣨AND �߼���
    for (const auto& cond : conditions)
    {
        int idx = getFieldIndex(def, cond.field);
        if (idx == -1 || static_cast<size_t>(idx) >= record.size())
            return false;

        const std::string& value = record[idx];
        bool match = false; // ���ڱ�ǵ�ǰ�����Ƿ�ƥ��

        // �����ֶ����ͽ��бȽ�
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

        // �����ǰ������ƥ�䣬ֱ�ӷ��� false
        if (!match)
            return false;
    }

    // ����������ƥ��
    return true;
}

// ��ȡ�ֶ��ڼ�¼�е�λ��
int TableManager::getFieldIndex(const TableDef& def, const std::string& fieldName)
{
    for (int i = 0; i < static_cast<int>(def.fields.size()); i++)
    {
        if (def.fields[i].name == fieldName)
            return i;
    }
    return -1;
}

// �����ѯʵ��
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

    // ȷ��Ҫ���ص��ֶ�����
    std::vector<int> fieldIndices;
    if (fields.empty())
    {
        // ���������ֶ�
        for (int i = 0; i < static_cast<int>(def.fields.size()); i++)
            fieldIndices.push_back(i);
    }
    else
    {
        // ����ָ���ֶ�
        for (const auto& field : fields)
        {
            int idx = getFieldIndex(def, field);
            if (idx != -1)
                fieldIndices.push_back(idx);
        }
    }

    // ȷ���ļ��Ǵ򿪵�
    if (!it->second->open_tree_file())
    {
        std::cerr << "Failed to open table file" << std::endl;
        return results;
    }

    try
    {
        // ���� B+ ����ȡ���м�¼
        auto records = it->second->traverse();
        std::cout << "Found " << records.size() << " records in table" << std::endl;

        // ����ÿ����¼
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

    // �ر��ļ�
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

        // �Ƴ����ţ�������ڣ�
        if (value.front() == '\'' || value.front() == '"')
            value = value.substr(1, value.length() - 2);

        // 4�ֽڶ���
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
            // ȷ������ʱ���ʽ��ȷ
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
        // 4�ֽڶ���
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
                // �������
                result.push_back("'" + value + "'");
                offset += field.size;
                break;
            }
            case FieldType::DATETIME:
            {
                std::string value(data.data + offset);
                // �������
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

    // д��������
    ofs << tableDefs.size() << std::endl;

    // д��ÿ����Ķ���
    for (const auto& pair : tableDefs)
    {
        const auto& def = pair.second;

        // д�����
        ofs << def.tableName << std::endl;

        // д���ֶ�����
        ofs << def.fields.size() << std::endl;

        // д��ÿ���ֶεĶ���
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

        // д���ע��
        ofs << def.comment << std::endl;

        // д�봴��ʱ��͸���ʱ��
        ofs << def.create_time << std::endl;
        ofs << def.update_time << std::endl;

        // д�����������
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
    // ��ȡ�������
    std::getline(ifs, line);
    size_t tableCount = std::stoi(line);

    for (size_t i = 0; i < tableCount; i++)
    {
        // ��ȡ����
        std::string tableName;
        std::getline(ifs, tableName);
        if (tableName.empty())
            continue;

        TableDef def;
        def.tableName = tableName;

        // ��ȡ�ֶ�����
        std::getline(ifs, line);
        size_t fieldCount = std::stoi(line);

        // ��ȡÿ���ֶεĶ���
        for (size_t j = 0; j < fieldCount; j++)
        {
            std::getline(ifs, line);
            std::istringstream iss(line);

            FieldDef field;
            int type;

            // ��ȡ�ֶ�����������Ϣ
            if (!(iss >> field.name >> type >> field.size >>
                field.is_key >> field.is_nullable >>
                field.is_unique >> field.is_index))
            {
                std::cerr << "Failed to read field properties for field " << j
                    << " in table " << tableName << std::endl;
                continue;
            }

            // ��ȡĬ��ֵ��ʣ����������ݣ�
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

        // ��ȡ��ע��
        std::getline(ifs, def.comment);

        // ��ȡ����ʱ��͸���ʱ��
        std::getline(ifs, def.create_time);
        std::getline(ifs, def.update_time);

        // ��ȡ����������
        std::string endMark;
        std::getline(ifs, endMark);
        if (endMark != "END_TABLE")
        {
            std::cerr << "Invalid table definition format for table: " << tableName << std::endl;
            continue;
        }

        // �򿪱��ļ�
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
    // �������
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

    // ������ֶ�
    if (!newDef.addField(fieldName, type, size))
    {
        std::cerr << "Failed to add field: " << fieldName << std::endl;
        return false;
    }

    // ������ʱ��Ǩ������
    if (!rebuildTable(cleanTableName, oldDef, newDef))
    {
        std::cerr << "Failed to rebuild table" << std::endl;
        return false;
    }

    // ���±���
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

    // ������ʱ��Ǩ������
    if (!rebuildTable(tableName, oldDef, newDef, oldName, newName))
    {
        std::cerr << "Failed to rebuild table" << std::endl;
        return false;
    }

    // ���±���
    tableDefs[tableName] = newDef;
    saveTableDefs();
    return true;
}

bool TableManager::alterFieldType(const std::string& tableName,
    const std::string& fieldName,
    FieldType newType, size_t newSize)
{
    // �������
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

    // ������ʱ��Ǩ������
    if (!rebuildTable(cleanTableName, oldDef, newDef))
    {
        std::cerr << "Failed to rebuild table" << std::endl;
        return false;
    }

    // ���±���
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
    // ������ʱ��
    std::string tempTableName = tableName + "_temp";
    TableDef tempDef = newDef;
    tempDef.tableName = tempTableName;

    // ������ʱ��
    if (!createTable(tempDef))
    {
        std::cerr << "Failed to create temporary table" << std::endl;
        return false;
    }

    // ��ȡԭ������
    auto records = select(tableName);
    std::cout << "Read " << records.size() << " records from original table" << std::endl;

    // ת�����������ݵ��±�
    for (const auto& record : records)
    {
        std::vector<std::string> newRecord(tempDef.fields.size());

        // ��ʼ���¼�¼�������ֶ�ΪĬ��ֵ
        for (size_t j = 0; j < tempDef.fields.size(); j++)
        {
            const auto& newField = tempDef.fields[j];
            newRecord[j] = newField.default_value.empty() ? "NULL" : newField.default_value;
        }

        // ӳ����ֶε����ֶ�
        for (size_t i = 0; i < oldDef.fields.size(); i++)
        {
            const auto& oldField = oldDef.fields[i];

            // ���±����в��Ҷ�Ӧ�ֶ�
            for (size_t j = 0; j < tempDef.fields.size(); j++)
            {
                const auto& newField = tempDef.fields[j];

                // ����ֶ����Ƿ�ƥ�䣨�����������������
                if (newField.name == oldField.name ||
                    (!oldFieldName.empty() && oldField.name == oldFieldName &&
                        newField.name == newFieldName))
                {
                    // ����ԭʼֵ�ĸ�ʽ���������ţ�
                    newRecord[j] = record[i];
                    break;
                }
            }
        }

        // ��ӡ������Ϣ
        std::cout << "Inserting into table: " << tempTableName << std::endl;
        std::cout << "Values: ";
        for (const auto& val : newRecord)
        {
            std::cout << val << " ";
        }
        std::cout << std::endl;

        // �������ݵ���ʱ��
        if (!insert(tempTableName, newRecord))
        {
            std::cerr << "Failed to insert record into temporary table" << std::endl;
            dropTable(tempTableName);
            return false;
        }
    }

    // �ر�ԭ�����ʱ��
    if (tables[tableName])
        tables[tableName]->close_tree_file();
    if (tables[tempTableName])
        tables[tempTableName]->close_tree_file();

    // ɾ��ԭ���ļ�
    std::string oldFileName = dbPath + tableName + ".tbl";
    std::string tempFileName = dbPath + tempTableName + ".tbl";

#ifdef _WIN32
    _unlink(oldFileName.c_str());
#else
    unlink(oldFileName.c_str());
#endif

    // ��������ʱ���ļ�
    if (rename(tempFileName.c_str(), oldFileName.c_str()) != 0)
    {
        std::cerr << "Failed to rename temporary table file" << std::endl;
        return false;
    }

    // ���±���
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

    // �رձ��ļ�
    if (tables[oldName])
        tables[oldName]->close_tree_file();

    // �������ļ�
    std::string oldFileName = dbPath + oldName + ".tbl";
    std::string newFileName = dbPath + newName + ".tbl";
    if (rename(oldFileName.c_str(), newFileName.c_str()) != 0)
    {
        std::cerr << "Failed to rename table file" << std::endl;
        return false;
    }

    // �����ڴ��еı���
    TableDef def = it->second;
    def.tableName = newName;
    tableDefs[newName] = def;
    tableDefs.erase(oldName);

    // ����B+������
    auto* oldTree = tables[oldName];
    tables.erase(oldName);
    tables[newName] = new bpt::bplus_tree(newFileName.c_str());
    delete oldTree;

    saveTableDefs();
    return true;
}

bool TableManager::deleteRecords(const std::string& tableName, const std::string& where)
{
    // �������
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

    // ����WHERE����
    auto conditions = parseWhereClause(where);

    // ��ȡ����ƥ��ļ�¼
    auto records = select(cleanTableName, std::vector<std::string>(), where);
    std::cout << "Found " << records.size() << " records to delete" << std::endl;

    // ɾ��ƥ��ļ�¼
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
    // ����������Ƴ��ո�ͷֺţ�
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

    // ��ȡҪ���µļ�¼
    auto records = select(cleanTableName, std::vector<std::string>(), where);
    std::cout << "Found " << records.size() << " records to update" << std::endl;

    // ����ÿ����¼
    for (const auto& record : records)
    {
        std::vector<std::string> newRecord = record;

        // Ӧ�ø���
        for (const auto& setValue : setValues)
        {
            int fieldIndex = getFieldIndex(tableDefIt->second, setValue.first);
            if (fieldIndex >= 0)
            {
                // ����ֵ�е�����
                std::string value = setValue.second;
                if (value.front() == '\'' && value.back() == '\'')
                {
                    value = value.substr(1, value.length() - 2);
                }
                newRecord[fieldIndex] = value;
            }
        }

        // ɾ���ɼ�¼
        bpt::key_t oldKey(record[0].c_str());
        it->second->remove(oldKey);

        // �����¼�¼
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

    // ��ȡ������Ķ���
    auto def1 = tableDefs.find(table1);
    auto def2 = tableDefs.find(table2);
    if (def1 == tableDefs.end() || def2 == tableDefs.end())
    {
        std::cerr << "Table not found" << std::endl;
        return results;
    }

    // ������������
    JoinCondition jc = parseJoinCondition(joinCondition);
    if (jc.table1Field.empty() || jc.table2Field.empty())
    {
        std::cerr << "Invalid join condition" << std::endl;
        return results;
    }

    // ��ȡ�����������
    auto records1 = select(table1);
    auto records2 = select(table2);

    // ��ȡ�����ֶε�����
    int joinIndex1 = getFieldIndex(def1->second, jc.table1Field);
    int joinIndex2 = getFieldIndex(def2->second, jc.table2Field);

    if (joinIndex1 < 0 || joinIndex2 < 0)
    {
        std::cerr << "Join field not found" << std::endl;
        return results;
    }

    // ִ�����Ӳ���
    for (const auto& record1 : records1)
    {
        for (const auto& record2 : records2)
        {
            // �����������
            if (record1[joinIndex1] == record2[joinIndex2])
            {
                std::vector<std::string> joinedRecord;

                // ���ָ�����ֶΣ�ֻѡ��ָ�����ֶ�
                if (!fields.empty())
                {
                    for (const auto& field : fields)
                    {
                        // ����ֶ������ĸ���
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
                    // ���û��ָ���ֶΣ��ϲ������ֶ�
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

    // ������ʽ: table1.field1 = table2.field2
    size_t equalPos = condition.find('=');
    if (equalPos == std::string::npos)
        return jc;

    std::string left = condition.substr(0, equalPos);
    std::string right = condition.substr(equalPos + 1);

    // ����ո�
    left = left.substr(left.find_first_not_of(" \t"));
    left = left.substr(0, left.find_last_not_of(" \t") + 1);
    right = right.substr(right.find_first_not_of(" \t"));
    right = right.substr(0, right.find_last_not_of(" \t") + 1);

    // �����ֶ���
    size_t dotPos = left.find('.');
    if (dotPos != std::string::npos)
        jc.table1Field = left.substr(dotPos + 1);

    dotPos = right.find('.');
    if (dotPos != std::string::npos)
        jc.table2Field = right.substr(dotPos + 1);

    return jc;
}