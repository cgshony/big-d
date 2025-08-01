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
        assert self
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
        return f"DataFrame (2x3)\n{tables}"
    
    def __bool__(self):
        return bool(self.column_content)

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


from datetime import datetime 
df = DataFrame({"date": [datetime.strptime('2025-05-06', '%Y-%m-%d'),
                         datetime.strptime('2025-05-09', '%Y-%m-%d'),
                         datetime.strptime('2025-05-08', '%Y-%m-%d'),
                         datetime.strptime('2025-05-07', '%Y-%m-%d'),
                         datetime.strptime('2025-05-05', '%Y-%m-%d'),]})

def validate_column_types(**column_restrictions):
    def decorator(func):
        def decorated(df, **kwargs):
            validate_dataframe(df)
            for column, column_type in column_restrictions.items():
                isinstance(df[column][0], column_type)
                raise TypeError(f"Type for column {column}, must be {column_type}")
            return func(df, **kwargs)
        return decorated
    return decorator

@validate_column_types(date=datetime, age=int)
def extract_time_interval(df):
    dates = sorted(df["date"])
    return dates[-1] - dates[0]

def validate_dataframe(obj):
    if not isinstance(obj, DataFrame):
        raise TypeError

def require_non_empty(func):
    def decorated(df, **kwargs):
        validate_dataframe(df)
        if not df:
            raise TypeError(f"Empty DataFrame not allowed for {func.__name__}")
        return func(df, **kwargs)
    return decorated


@require_non_empty
def avg(df, /, column_name):
    column = df[column_name]
    return sum(column) / len(column)

print(avg(DataFrame({'number':[1, 2, 3, 4, 7]})), column_number='number')