#pragma once
#ifndef BPT_H
#define BPT_H

#include <sys/types.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <cstddef>
#include "predefined.h"
#include <iostream>
#include <direct.h> // for _mkdir
#include <io.h>
#include <vector>

#ifndef UNIT_TEST

#else
// #include "unit_test_predefined.h"
#endif

namespace bpt
{

    /* offsets */
#define OFFSET_META 0
#define OFFSET_BLOCK OFFSET_META + sizeof(meta_t)
#define SIZE_NO_CHILDREN sizeof(leaf_node_t) - BP_ORDER * sizeof(record_t)

    /* meta information of B+ tree */
    typedef struct
    {
        size_t order;             /* `order` of B+ tree */
        size_t value_size;        /* size of value */
        size_t key_size;          /* size of key */
        size_t internal_node_num; /* how many internal nodes */
        size_t leaf_node_num;     /* how many leafs */
        size_t height;            /* height of tree (exclude leafs) */
        off_t slot;               /* where to store new block */
        off_t root_offset;        /* where is the root of internal nodes */
        off_t leaf_offset;        /* where is the first leaf */
    } meta_t;

    /* internal nodes' index segment */
    struct index_t
    {
        key_t key;
        off_t child; /* child's offset */
    };

    /***
     * internal node block
     ***/
    struct internal_node_t
    {
        typedef index_t* child_t;

        off_t parent; /* parent node offset */
        off_t next;
        off_t prev;
        size_t n; /* how many children */
        index_t children[BP_ORDER];
    };

    /* the final record of value */
    struct record_t
    {
        key_t key;
        value_t value;

        record_t() : value()
        {
            std::cout << "record_t default constructor" << std::endl;
        }

        ~record_t()
        {
            std::cout << "record_t destructor for key: " << key.k << std::endl;
        }

        record_t(const record_t& other) : key(other.key), value()
        {
            std::cout << "record_t copy constructor for key: " << other.key.k << std::endl;
            if (other.value.data && other.value.size > 0)
            {
                try
                {
                    std::cout << "Copying value of size: " << other.value.size << std::endl;
                    value.size = other.value.size;
                    value.data = new char[value.size];
                    std::memcpy(value.data, other.value.data, value.size);
                    std::cout << "Value copied successfully" << std::endl;
                }
                catch (...)
                {
                    std::cerr << "Failed to copy value data" << std::endl;
                    value.data = nullptr;
                    value.size = 0;
                    throw;
                }
            }
            else
            {
                std::cout << "No value data to copy" << std::endl;
            }
        }

        record_t& operator=(const record_t& other)
        {
            std::cout << "record_t assignment operator for key: " << other.key.k << std::endl;
            if (this != &other)
            {
                key = other.key;

                // 保存旧数据
                char* old_data = value.data;
                size_t old_size = value.size;

                try
                {
                    if (other.value.data && other.value.size > 0)
                    {
                        std::cout << "Copying value of size: " << other.value.size << std::endl;
                        value.data = new char[other.value.size];
                        value.size = other.value.size;
                        std::memcpy(value.data, other.value.data, value.size);
                        std::cout << "Value copied successfully" << std::endl;
                    }
                    else
                    {
                        value.data = nullptr;
                        value.size = 0;
                        std::cout << "No value data to copy" << std::endl;
                    }

                    // 删除旧数据
                    delete[] old_data;
                }
                catch (...)
                {
                    std::cerr << "Failed to copy value data" << std::endl;
                    value.data = old_data;
                    value.size = old_size;
                    throw;
                }
            }
            return *this;
        }

        // 序列化到文件
        bool serialize(FILE* fp) const
        {
            if (!fp)
                return false;

            // 写入键
            if (fwrite(&key, sizeof(key_t), 1, fp) != 1)
                return false;

            // 写入值
            return value.serialize(fp);
        }

        // 从文件反序列化
        bool deserialize(FILE* fp)
        {
            if (!fp)
                return false;

            // 读取键
            if (fread(&key, sizeof(key_t), 1, fp) != 1)
                return false;

            // 读取值
            return value.deserialize(fp);
        }
    };

    /* leaf node block */
    struct leaf_node_t
    {
        typedef record_t* child_t;

        off_t parent; /* parent node offset */
        off_t next;
        off_t prev;
        size_t n;
        record_t children[BP_ORDER];
    };

    /* the encapulated B+ tree */
    class bplus_tree
    {
    public:
        bplus_tree(const char* path, bool force_empty = false);

        /* abstract operations */
        int search(const key_t& key, value_t* value) const;
        int search_range(key_t* left, const key_t& right,
            value_t* values, size_t max, bool* next = NULL) const;
        int remove(const key_t& key);
        int insert(const key_t& key, value_t value);
        int update(const key_t& key, value_t value);

        meta_t get_meta() const
        {
            return meta;
        }

        // 获取第一个叶子节点的偏移量
        off_t get_first_leaf() const
        {
            return meta.leaf_offset;
        }

        // 读取叶子节点
        bool read_leaf_node(leaf_node_t* leaf, off_t offset) const
        {
            return map(leaf, offset) == 0;
        }

        // 打开文件并返回是否成功
        bool open_tree_file(const char* mode = "rb+") const
        {
            open_file(mode);
            return fp != nullptr;
        }

        // 关闭文件
        void close_tree_file() const
        {
            if (fp)
            {
                fflush(fp);
#ifdef _WIN32
                _commit(_fileno(fp));
#else
                fsync(fileno(fp));
#endif
                fclose(fp);
                fp = nullptr;
                fp_level = 0;
            }
        }

        ~bplus_tree()
        {
            close_tree_file();
        }

        // 添加遍历函数
        std::vector<record_t> traverse()
        {
            std::vector<record_t> records;
            if (!fp)
            {
                std::cerr << "File not open for traversal" << std::endl;
                return records;
            }

            leaf_node_t leaf;
            off_t offset = meta.leaf_offset;
            std::cout << "Starting traverse from leaf offset: " << offset << std::endl;

            while (offset != 0)
            {
                if (map(&leaf, offset) != 0)
                {
                    std::cerr << "Failed to read leaf node at offset: " << offset << std::endl;
                    break;
                }

                std::cout << "Reading leaf node with " << leaf.n << " records" << std::endl;
                for (int i = 0; i < leaf.n; i++)
                {
                    records.push_back(leaf.children[i]);
                    std::cout << "Read record with key: " << leaf.children[i].key.k << std::endl;
                }
                offset = leaf.next;
            }

            std::cout << "Traverse complete, found " << records.size() << " records" << std::endl;
            return records;
        }

#ifndef UNIT_TEST
    private:
#else
    public:
#endif
        char path[512];
        meta_t meta;

        /* init empty tree */
        void init_from_empty();

        /* find index */
        off_t search_index(const key_t& key) const;

        /* find leaf */
        off_t search_leaf(off_t index, const key_t& key) const;
        off_t search_leaf(const key_t& key) const
        {
            return search_leaf(search_index(key), key);
        }

        /* remove internal node */
        void remove_from_index(off_t offset, internal_node_t& node,
            const key_t& key);

        /* borrow one key from other internal node */
        bool borrow_key(bool from_right, internal_node_t& borrower,
            off_t offset);

        /* borrow one record from other leaf */
        bool borrow_key(bool from_right, leaf_node_t& borrower);

        /* change one's parent key to another key */
        void change_parent_child(off_t parent, const key_t& o, const key_t& n);

        /* merge right leaf to left leaf */
        void merge_leafs(leaf_node_t* left, leaf_node_t* right);

        void merge_keys(index_t* where, internal_node_t& left,
            internal_node_t& right);

        /* insert into leaf without split */
        void insert_record_no_split(leaf_node_t* leaf,
            const key_t& key, const value_t& value);

        /* add key to the internal node */
        void insert_key_to_index(off_t offset, const key_t& key,
            off_t value, off_t after);
        void insert_key_to_index_no_split(internal_node_t& node, const key_t& key,
            off_t value);

        /* change children's parent */
        void reset_index_children_parent(index_t* begin, index_t* end,
            off_t parent);

        template <class T>
        void node_create(off_t offset, T* node, T* next);

        template <class T>
        void node_remove(T* prev, T* node);

        /* multi-level file open/close */
        mutable FILE* fp;
        mutable int fp_level;
        void open_file(const char* mode = "rb+") const
        {
            std::cout << "Opening file: " << path << " mode: " << mode << std::endl;

            if (fp_level == 0)
            {
                if (fp)
                {
                    fclose(fp);
                    fp = nullptr;
                }

                // 如果是写模式，先尝试创建目录
                if (strcmp(mode, "wb+") == 0)
                {
                    std::string dir = std::string(path).substr(0, std::string(path).find_last_of("/\\"));
#ifdef _WIN32
                    _mkdir(dir.c_str());
#else
                    mkdir(dir.c_str(), 0777);
#endif
                }

#ifdef _WIN32
                fopen_s(&fp, path, mode);
#else
                fp = fopen(path, mode);
#endif

                if (!fp && strcmp(mode, "rb+") == 0)
                {
                    // 如果以读写方式打开失败，尝试创建新文件
#ifdef _WIN32
                    fopen_s(&fp, path, "wb+");
#else
                    fp = fopen(path, "wb+");
#endif
                }

                if (!fp)
                {
                    std::cerr << "Failed to open file: " << path << std::endl;
                    return;
                }
            }

            ++fp_level;
            std::cout << "fp_level increased to: " << fp_level << std::endl;
        }

        void close_file() const
        {
            std::cout << "Closing file, current fp_level: " << fp_level << std::endl;
            if (fp_level == 1)
            {
                if (fp)
                {
                    fflush(fp); // 确保数据写入磁盘
                    fclose(fp);
                    fp = nullptr;
                }
            }
            if (fp_level > 0)
            {
                --fp_level;
            }
            std::cout << "fp_level decreased to: " << fp_level << std::endl;
        }

        /* alloc from disk */
        off_t alloc(size_t size)
        {
            off_t slot = meta.slot;
            meta.slot += size;
            return slot;
        }

        off_t alloc(leaf_node_t* leaf)
        {
            leaf->n = 0;
            meta.leaf_node_num++;
            return alloc(sizeof(leaf_node_t));
        }

        off_t alloc(internal_node_t* node)
        {
            node->n = 1;
            meta.internal_node_num++;
            return alloc(sizeof(internal_node_t));
        }

        void unalloc(leaf_node_t* leaf, off_t offset)
        {
            --meta.leaf_node_num;
        }

        void unalloc(internal_node_t* node, off_t offset)
        {
            --meta.internal_node_num;
        }
        // read from disk
        int map(void* block, off_t offset, size_t size) const
        {
            if (!block || size == 0)
                return -1;

            if (!fp && fp_level == 0)
            {
                open_file();
            }

            if (!fp)
                return -1;

            std::cout << "Reading from offset: " << offset << ", size: " << size << std::endl;

            if (fseek(fp, offset, SEEK_SET) != 0)
            {
                std::cerr << "Failed to seek to offset " << offset << std::endl;
                return -1;
            }

            // 如果是叶子节点，需要特殊处理
            if (size == sizeof(leaf_node_t))
            {
                leaf_node_t* leaf = static_cast<leaf_node_t*>(block);

                // 读取基本信息
                if (fread(&leaf->parent, sizeof(off_t), 1, fp) != 1 ||
                    fread(&leaf->next, sizeof(off_t), 1, fp) != 1 ||
                    fread(&leaf->prev, sizeof(off_t), 1, fp) != 1 ||
                    fread(&leaf->n, sizeof(size_t), 1, fp) != 1)
                {
                    return -1;
                }

                // 读取每条记录
                for (size_t i = 0; i < leaf->n; i++)
                {
                    if (!leaf->children[i].deserialize(fp))
                    {
                        return -1;
                    }
                }

                return 0;
            }

            // 其他类型的节点直接读取
            size_t rd = fread(block, size, 1, fp);
            if (rd != 1)
            {
                if (offset == OFFSET_META)
                    return 1;
                return -1;
            }

            return 0;
        }

        template <class T>
        int map(T* block, off_t offset) const
        {
            return map(block, offset, sizeof(T));
        }

        /* write block to disk */
        int unmap(void* block, off_t offset, size_t size) const
        {
            if (!fp && fp_level == 0)
            {
                open_file("rb+");
            }

            if (!fp)
                return -1;

            if (fseek(fp, offset, SEEK_SET) != 0)
                return -1;

            // 如果是叶子节点，需要特殊处理
            if (size == sizeof(leaf_node_t))
            {
                leaf_node_t* leaf = static_cast<leaf_node_t*>(block);

                // 写入基本信息
                if (fwrite(&leaf->parent, sizeof(off_t), 1, fp) != 1 ||
                    fwrite(&leaf->next, sizeof(off_t), 1, fp) != 1 ||
                    fwrite(&leaf->prev, sizeof(off_t), 1, fp) != 1 ||
                    fwrite(&leaf->n, sizeof(size_t), 1, fp) != 1)
                {
                    return -1;
                }

                // 写入每条记录
                for (size_t i = 0; i < leaf->n; i++)
                {
                    if (!leaf->children[i].serialize(fp))
                    {
                        return -1;
                    }
                }
            }
            else
            {
                // 其他类型的节点直接写入
                if (fwrite(block, size, 1, fp) != 1)
                    return -1;
            }

            fflush(fp);

#ifdef _WIN32
            _commit(_fileno(fp));
#else
            fsync(fileno(fp));
#endif

            return 0;
        }
        template <class T>
        int unmap(T* block, off_t offset) const
        {
            return unmap(block, offset, sizeof(T));
        }
    };

}

#endif /* end of BPT_H */
