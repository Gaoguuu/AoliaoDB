#ifndef PREDEFINED_H
#define PREDEFINED_H
#define _CRT_SECURE_NO_WARNINGS
#include <string.h>
#include <algorithm>

namespace bpt
{

    /* predefined B+ info */
#define BP_ORDER 50

    /* key/value type */

#pragma pack(push, 1)
    struct value_t
    {
        char* data;
        size_t size;

        value_t() : data(nullptr), size(0) {}

        ~value_t()
        {
            clear();
        }

        void clear()
        {
            delete[] data;
            data = nullptr;
            size = 0;
        }

        // 序列化到文件
        bool serialize(FILE* fp) const
        {
            if (!fp)
                return false;

            // 写入大小
            if (fwrite(&size, sizeof(size_t), 1, fp) != 1)
                return false;

            // 写入数据
            if (size > 0 && data)
            {
                if (fwrite(data, size, 1, fp) != 1)
                    return false;
            }

            return true;
        }

        // 从文件反序列化
        bool deserialize(FILE* fp)
        {
            if (!fp)
                return false;

            // 清理现有数据
            clear();

            // 读取大小
            if (fread(&size, sizeof(size_t), 1, fp) != 1)
                return false;

            // 读取数据
            if (size > 0)
            {
                try
                {
                    data = new char[size];
                    if (fread(data, size, 1, fp) != 1)
                    {
                        clear();
                        return false;
                    }
                }
                catch (...)
                {
                    clear();
                    return false;
                }
            }

            return true;
        }

        bool is_valid() const
        {
            return data != nullptr && size > 0 && size < 1024 * 1024; // 使用合理的大小限制
        }

        value_t(const value_t& other) : data(nullptr), size(0)
        {
            if (other.data && other.size > 0)
            {
                size = other.size;
                data = new char[size];
                std::memcpy(data, other.data, size);
            }
        }

        value_t& operator=(const value_t& other)
        {
            if (this != &other)
            {
                clear();
                if (other.data && other.size > 0)
                {
                    size = other.size;
                    data = new char[size];
                    std::memcpy(data, other.data, size);
                }
            }
            return *this;
        }

        value_t(value_t&& other) noexcept : data(other.data), size(other.size)
        {
            other.data = nullptr;
            other.size = 0;
        }

        value_t& operator=(value_t&& other) noexcept
        {
            if (this != &other)
            {
                clear();
                data = other.data;
                size = other.size;
                other.data = nullptr;
                other.size = 0;
            }
            return *this;
        }
    };
#pragma pack(pop)

    // typedef int value_t;
    struct key_t
    {
        char k[16];

        key_t(const char* str = "")
        {
            memset(k, 0, sizeof(k));
            strcpy_s(k, str);
        }
    };

    inline int keycmp(const key_t& a, const key_t& b)
    {
        int x = strlen(a.k) - strlen(b.k);
        return x == 0 ? strcmp(a.k, b.k) : x;
    }

#define OPERATOR_KEYCMP(type)                      \
    bool operator<(const key_t &l, const type &r)  \
    {                                              \
        return keycmp(l, r.key) < 0;               \
    }                                              \
    bool operator<(const type &l, const key_t &r)  \
    {                                              \
        return keycmp(l.key, r) < 0;               \
    }                                              \
    bool operator==(const key_t &l, const type &r) \
    {                                              \
        return keycmp(l, r.key) == 0;              \
    }                                              \
    bool operator==(const type &l, const key_t &r) \
    {                                              \
        return keycmp(l.key, r) == 0;              \
    }

}

#endif /* end of PREDEFINED_H */
