#pragma once
#include <string>
#include <vector>
#include <ctime>

// �ֶ�����ö��
enum class FieldType
{
    INT,
    VARCHAR,
    FLOAT,
    DOUBLE,
    DATETIME, // ��������ʱ������
    BOOL      // ������������
};

// �ֶζ���ṹ
struct FieldDef
{
    std::string name;          // �ֶ���
    FieldType type;            // �ֶ�����
    size_t size;               // �ֶγ��ȣ����� VARCHAR ���ͣ�
    bool is_key;               // �Ƿ�Ϊ��
    bool is_nullable;          // �Ƿ�����Ϊ��
    bool is_unique;            // �Ƿ�Ψһ
    bool is_index;             // �Ƿ�������
    std::string default_value; // Ĭ��ֵ

    FieldDef() : size(0), is_key(false), is_nullable(true),
        is_unique(false), is_index(false) {}
};

// ������
class TableDef
{
public:
    std::string tableName;        // ����
    std::vector<FieldDef> fields; // �ֶζ����б�
    std::string comment;          // ��ע��
    std::string create_time;      // ����ʱ��
    std::string update_time;      // ����ʱ��

    // ����ֶ�
    bool addField(const std::string& name, FieldType type, size_t size = 0,
        bool is_key = false, bool is_nullable = true,
        bool is_unique = false, bool is_index = false,
        const std::string& default_value = "")
    {
        // ����ֶ����Ƿ��Ѵ���
        for (const auto& field : fields)
        {
            if (field.name == name)
                return false;
        }

        FieldDef field;
        field.name = name;
        field.type = type;
        field.size = size;
        field.is_key = is_key;
        field.is_nullable = is_nullable;
        field.is_unique = is_unique;
        field.is_index = is_index;
        field.default_value = default_value;

        // ����Ĭ�ϴ�С
        if (type == FieldType::VARCHAR && size == 0)
            field.size = 256; // VARCHAR Ĭ�ϳ���
        else if (type == FieldType::INT)
            field.size = sizeof(int);
        else if (type == FieldType::FLOAT)
            field.size = sizeof(float);
        else if (type == FieldType::DOUBLE)
            field.size = sizeof(double);
        else if (type == FieldType::DATETIME)
            field.size = sizeof(time_t);
        else if (type == FieldType::BOOL)
            field.size = sizeof(bool);

        fields.push_back(field);
        return true;
    }

    // �Ƴ��ֶ�
    bool removeField(const std::string& name)
    {
        for (auto it = fields.begin(); it != fields.end(); ++it)
        {
            if (it->name == name)
            {
                if (it->is_key)
                    return false; // ������ɾ������
                fields.erase(it);
                return true;
            }
        }
        return false;
    }

    // �����¼��С
    size_t calculateRecordSize() const
    {
        size_t totalSize = 0;
        for (const auto& field : fields)
        {
            size_t fieldSize = 0;
            switch (field.type)
            {
            case FieldType::INT:
                fieldSize = sizeof(int);
                break;
            case FieldType::VARCHAR:
                fieldSize = field.size + sizeof(size_t); // �����ַ�������
                break;
            case FieldType::FLOAT:
                fieldSize = sizeof(float);
                break;
            case FieldType::DOUBLE:
                fieldSize = sizeof(double);
                break;
            case FieldType::DATETIME:
                fieldSize = sizeof(time_t);
                break;
            case FieldType::BOOL:
                fieldSize = sizeof(bool);
                break;
            }
            totalSize = (totalSize + 3) & ~3; // 4�ֽڶ���
            totalSize += fieldSize;
        }
        totalSize = (totalSize + 3) & ~3; // ���մ�С4�ֽڶ���
        return totalSize;
    }

    // �޸��ֶ���
    bool renameField(const std::string& oldName, const std::string& newName)
    {
        // ����������Ƿ��Ѵ���
        for (const auto& field : fields)
        {
            if (field.name == newName)
                return false;
        }

        // ���Ҳ��������ֶ�
        for (auto& field : fields)
        {
            if (field.name == oldName)
            {
                field.name = newName;
                return true;
            }
        }
        return false;
    }

    // �޸��ֶ�����
    bool alterFieldType(const std::string& name, FieldType newType, size_t newSize = 0)
    {
        for (auto& field : fields)
        {
            if (field.name == name)
            {
                if (field.is_key)
                {
                    return false; // �������޸���������
                }

                field.type = newType;
                if (newType == FieldType::VARCHAR)
                {
                    field.size = newSize > 0 ? newSize : 256;
                }
                else if (newType == FieldType::INT)
                {
                    field.size = sizeof(int);
                }
                else if (newType == FieldType::FLOAT)
                {
                    field.size = sizeof(float);
                }
                else if (newType == FieldType::DOUBLE)
                {
                    field.size = sizeof(double);
                }
                else if (newType == FieldType::DATETIME)
                {
                    field.size = sizeof(time_t);
                }
                else if (newType == FieldType::BOOL)
                {
                    field.size = sizeof(bool);
                }

                return true;
            }
        }
        return false;
    }

    // ��ȡ��¼��С
    size_t getRecordSize() const
    {
        return calculateRecordSize();
    }

    // ... �������з������ֲ��� ...
};