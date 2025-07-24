from prettytable import PrettyTable
from collections import defaultdict

class DataFrame:
    def __init__(self, column_content):
        for column in column_content.values():
            self.validate_column(column)
        self._validate_input(column_content)
        self.column_content = column_content

    @classmethod
    def from_rows(cls, rows):
        column_content = defaultdict(list)
        for row in rows:
            for column, value in row.items():
                column_content[column].append(value)
            return cls(column_content)

    def validate_column(self,value):
            item_type = type(column[0])
            for item in column[1:]:
                if not isinstance(item, item_type):
                    raise TypeError('Inconsistent column type')

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
        self.validate_column
        self.column_content[name] = value

    def __str__(self):
        """Represent DataFrame as a string with it's size and contents."""
        table = PrettyTable()
        table.field_names = self.column_definitions.keys()
        table.add_rows(list(zip(*self.column_definitions.values())))
        return f"DataFrame (2x3)\n{table!s}"

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