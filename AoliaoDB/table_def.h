#pragma once
#include <string>
#include <vector>

enum class FieldType
{
    INT,
    VARCHAR,
    FLOAT,
    DOUBLE
};

#pragma pack(push, 1) // ȷ��1�ֽڶ���
struct FieldDef
{
    std::string name;
    FieldType type;
    size_t size;
};

struct TableDef
{
    std::string tableName;
    std::vector<FieldDef> fields;
    size_t recordSize;

    void calculateRecordSize()
    {
        recordSize = 0;
        for (const auto& field : fields)
        {
            switch (field.type)
            {
            case FieldType::INT:
                recordSize = (recordSize + 3) & ~3; // 4�ֽڶ���
                recordSize += sizeof(int);
                break;
            case FieldType::VARCHAR:
                recordSize = (recordSize + 3) & ~3; // 4�ֽڶ���
                recordSize += field.size;
                break;
            }
        }
        recordSize = (recordSize + 3) & ~3; // ���մ�С4�ֽڶ���
    }
};
#pragma pack(pop)