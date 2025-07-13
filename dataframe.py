class DataFrame:
    def __init__(self, column_content):
        for column in column_content.values():
            self.validate_column(column)
        self._validate_input(column_content)
        self.column_content = column_content

    def validate_column(self):
            item_type = type(column[0])
            for item in column[1:]:
                if not isinstance(item, item_type):
                    raise TypeError('inconsistent column types')

    def _validate_input(self, column_content):
        if not isinstance(column_content, dict):
            raise ValueError('Please initialize a dictionary.')

    @property
    def shape(self):
        len_rows = max(len(row) for row in self.column_content.values())
        len_cols = len(self.column_content)
        return len_rows, len_cols

    def __getitem__(self, name):
        return self.column_content[name]
    
    def __setitem__(self, name, value):
        self.column_content[name] = value


df = DataFrame({"name": ["Гошо", "Пешо"], "age": [30, 16]})
print(df.shape) # (2, 2)
print(df["name"]) # ["Гошо", "Пешо"]
df["height"] = [175, "по-висок от Стан"]
print(df.shape) # (2, 3)
print(df['height'])
# DataFrame (2x3)
# +------+-----+--------+
# | name | age | height |
# +------+-----+--------+
# | Гошо | 30 | 175 |
# | Пешо | 16 | 196 |
# +------+-----+--------+
