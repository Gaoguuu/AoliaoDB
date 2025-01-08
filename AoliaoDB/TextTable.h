#pragma once
#include <iostream>
#include <map>
#include <iomanip>
#include <vector>
#include <string>

class TextTable
{

public:
    enum class Alignment
    {
        LEFT,
        RIGHT
    };
    typedef std::vector<std::string> Row;
    TextTable(char horizontal = '-', char vertical = '|', char corner = '+') : _horizontal(horizontal),
        _vertical(vertical),
        _corner(corner)
    {
    }

    void setAlignment(unsigned i, Alignment alignment)
    {
        _alignment[i] = alignment;
    }

    Alignment alignment(unsigned i) const
    {
        return _alignment[i];
    }

    char vertical() const
    {
        return _vertical;
    }

    char horizontal() const
    {
        return _horizontal;
    }

    void add(std::string const& content)
    {
        _current.push_back(content);
        while (_width.size() < _current.size())
        {
            _width.push_back(0);
        }
        size_t curr_width = _width[_current.size() - 1];
        _width[_current.size() - 1] = std::max<size_t>(curr_width, content.size());
    }

    void endOfRow()
    {
        if (!_current.empty())
        {
            _rows.push_back(_current);
            _current.clear();
        }
    }

    template <typename Iterator>
    void addRow(Iterator begin, Iterator end)
    {
        for (auto i = begin; i != end; ++i)
        {
            add(*i);
        }
        endOfRow();
    }

    template <typename Container>
    void addRow(Container const& container)
    {
        addRow(container.begin(), container.end());
    }

    std::vector<Row> const& rows() const
    {
        return _rows;
    }

    void setup() const
    {
        determineWidths();
        setupAlignment();
    }

    std::string ruler() const
    {
        std::string result;
        result += _corner;
        for (auto width = _width.begin(); width != _width.end(); ++width)
        {
            result += repeat(*width, _horizontal);
            result += _corner;
        }

        return result;
    }

    size_t width(size_t i) const
    {
        return _width[i];
    }

private:
    char _horizontal;
    char _vertical;
    char _corner;
    Row _current;
    std::vector<Row> _rows;
    std::vector<size_t> mutable _width;
    std::map<unsigned, Alignment> mutable _alignment;

    static std::string repeat(unsigned times, char c)
    {
        std::string result;
        for (; times > 0; --times)
            result += c;

        return result;
    }

    unsigned columns() const
    {
        return _rows[0].size();
    }

    void determineWidths() const
    {
        if (_rows.empty())
            return;

        _width.assign(_rows[0].size(), 0);

        for (const auto& row : _rows)
        {
            for (size_t i = 0; i < row.size(); ++i)
            {
                _width[i] = std::max(_width[i], row[i].size());
            }
        }
    }

    void setupAlignment() const
    {
        if (_rows.empty())
            return;

        for (size_t i = 0; i < _rows[0].size(); ++i)
        {
            if (_alignment.find(i) == _alignment.end())
            {
                _alignment[i] = Alignment::LEFT;
            }
        }
    }
};

std::ostream& operator<<(std::ostream& stream, TextTable const& table)
{
    if (table.rows().empty())
    {
        stream << "> No records found" << std::endl;
        return stream;
    }

    table.setup();
    stream << table.ruler() << "\n";

    for (const auto& row : table.rows())
    {
        stream << table.vertical();
        for (size_t i = 0; i < row.size(); ++i)
        {
            auto alignment = table.alignment(i) == TextTable::Alignment::LEFT ? std::left : std::right;
            stream << std::setw(table.width(i)) << alignment << row[i];
            stream << table.vertical();
        }
        stream << "\n";
        stream << table.ruler() << "\n";
    }

    return stream;
}
