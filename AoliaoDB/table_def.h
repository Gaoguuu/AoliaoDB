#pragma once
#include <string>
#include <vector>
#include <ctime>

// 字段类型枚举
enum class FieldType
{
    INT,
    VARCHAR,
    FLOAT,
    DOUBLE,
    DATETIME, // 新增日期时间类型
    BOOL      // 新增布尔类型
};

// 字段定义结构
struct FieldDef
{
    std::string name;          // 字段名
    FieldType type;            // 字段类型
    size_t size;               // 字段长度（对于 VARCHAR 类型）
    bool is_key;               // 是否为键
    bool is_nullable;          // 是否允许为空
    bool is_unique;            // 是否唯一
    bool is_index;             // 是否建立索引
    std::string default_value; // 默认值

    FieldDef() : size(0), is_key(false), is_nullable(true),
        is_unique(false), is_index(false) {}
};

// 表定义类
class TableDef
{
public:
    std::string tableName;        // 表名
    std::vector<FieldDef> fields; // 字段定义列表
    std::string comment;          // 表注释
    std::string create_time;      // 创建时间
    std::string update_time;      // 更新时间

    // 添加字段
    bool addField(const std::string& name, FieldType type, size_t size = 0,
        bool is_key = false, bool is_nullable = true,
        bool is_unique = false, bool is_index = false,
        const std::string& default_value = "")
    {
        // 检查字段名是否已存在
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

        // 设置默认大小
        if (type == FieldType::VARCHAR && size == 0)
            field.size = 256; // VARCHAR 默认长度
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

    // 移除字段
    bool removeField(const std::string& name)
    {
        for (auto it = fields.begin(); it != fields.end(); ++it)
        {
            if (it->name == name)
            {
                if (it->is_key)
                    return false; // 不允许删除主键
                fields.erase(it);
                return true;
            }
        }
        return false;
    }

    // 计算记录大小
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
                fieldSize = field.size + sizeof(size_t); // 包含字符串长度
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
            totalSize = (totalSize + 3) & ~3; // 4字节对齐
            totalSize += fieldSize;
        }
        totalSize = (totalSize + 3) & ~3; // 最终大小4字节对齐
        return totalSize;
    }

    // 修改字段名
    bool renameField(const std::string& oldName, const std::string& newName)
    {
        // 检查新名称是否已存在
        for (const auto& field : fields)
        {
            if (field.name == newName)
                return false;
        }

        // 查找并重命名字段
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

    // 修改字段类型
    bool alterFieldType(const std::string& name, FieldType newType, size_t newSize = 0)
    {
        for (auto& field : fields)
        {
            if (field.name == name)
            {
                if (field.is_key)
                {
                    return false; // 不允许修改主键类型
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

    // 获取记录大小
    size_t getRecordSize() const
    {
        return calculateRecordSize();
    }

    // ... 其他现有方法保持不变 ...
};