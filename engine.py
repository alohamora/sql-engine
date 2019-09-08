import os
import re
import sys
import itertools


class SQLEngine:
    DATA_FOLDER = "files"

    def __init__(self, query_str):
        self.query_str = query_str
        self.tables = {}
        self.query_columns = []
        self.condOp = None
        self.aggregationOp = None
        self.distinctOp = None
        self.query_tables = []
        self.query_conditions = []
        self.query_data = {"columns": [], "data": {}}
        self.projected_data = {"columns": [], "data": {}}
        self.operators = {
            "AND": lambda a,b: [val for val in a if val in b],
            "OR": lambda a,b: a + b,
            "<=": lambda a, b: a <= b,
            ">=": lambda a, b: a >= b,
            ">": lambda a, b: a > b,
            "<": lambda a, b: a < b,
            "=": lambda a, b: a == b,
        }

    @classmethod
    def execute_query(cls, query_str):
        __instance = cls(query_str.strip(" "))
        __instance.read_metadata()
        __instance.parse_query()
        __instance.run_query()

    def parse_query(self):
        self.handle_error(self.query_str[-1] == ";", "Semicolon not provided")
        self.query_str = self.query_str[:-1]
        tokens = self.query_str.lower().split(" ")
        case_tokens = self.query_str.split(" ")
        self.handle_error(tokens[0] == "select" and tokens.count("select") == 1)
        self.handle_error("from" in tokens and tokens.count("from") == 1)
        self.query_columns = self.parse_columns(
            " ".join(case_tokens[1 : tokens.index("from")])
        )
        if "where" in tokens:
            self.handle_error(
                tokens.index("where") != len(tokens) - 1,
                "No condition provided after where",
            )
            self.handle_error(
                tokens.index("where") - tokens.index("from") > 1,
                "No table names provided",
            )
            self.query_tables = self.parse_tables(
                " ".join(case_tokens[tokens.index("from") + 1 : tokens.index("where")])
            )
            self.query_conditions = self.parse_conditions(
                "".join(case_tokens[tokens.index("where") + 1 :])
            )
        else:
            self.handle_error(
                tokens.index("from") != len(tokens) - 1, "No table names provided"
            )
            self.query_tables = self.parse_tables(
                " ".join(case_tokens[tokens.index("from") + 1 :])
            )

    def parse_columns(self, colstr):
        colstr = colstr.strip(" ")
        if re.match("^distinct", colstr):
            colstr = colstr[8:]
            self.distinctOp = True
        columns = colstr.split(",")
        columns = [col.strip(" ") for col in columns if col != " " or col != ""]
        if len(columns) > 1:
            self.handle_error(
                all(re.match("^[\w*.-]+$", s) is not None for s in columns)
            )
        else:
            if re.match("^(sum|max|avg|min)(\([\w*.-]+\))$", columns[0]):
                self.aggregationOp = columns[0][:3]
                return [columns[0][4:-1]]
            else:
                self.handle_error(re.match("^[\w*.-]+$", columns[0]) is not None)
        return columns

    def parse_tables(self, tablestr):
        tables = tablestr.split(",")
        tables = [table.strip(" ") for table in tables if table != " " or table != ""]
        self.handle_error(all(re.match("^[\w-]+$", s) is not None for s in tables))
        self.handle_error(
            all(table in self.tables for table in tables), "Table not found"
        )
        return tables

    def parse_conditions(self, condstr):
        condstr = condstr.strip(" ")
        try:
            search = re.search("AND|OR", condstr)
            if search:
                self.condOp = search.group()
                condstr = re.sub("AND|OR", " ", condstr)
                conditions = re.split("\s+", condstr)
            else:
                conditions = [condstr]
            ret = []
            for cond in conditions:
                operator = None
                for op in ["<=", ">=", ">", "<", "="]:
                    opSearch = re.search(op, cond)
                    if opSearch:
                        operator = opSearch.group()
                        break
                self.handle_error(operator)
                cond = re.sub("<|>|<=|>=|=", " ", cond)
                parts = re.split("\s+", cond)
                ret.append((parts[0], operator, parts[1]))
        except:
            self.handle_error()
        return ret

    def run_query(self):
        self.join_tables()
        self.execute_conditions()
        if self.aggregationOp is not None:
            self.execute_aggregation(self.query_columns[0], self.query_tables[0])
        else:
            self.project_columns()
            self.display_table()

    def join_tables(self):
        columns = []
        for table in self.query_tables:
            temp = []
            for col in self.tables[table]["columns"]:
                self.query_data["columns"].append(table + "." + col)
                self.query_data["data"][table + "." + col] = []
                temp.append(self.tables[table]["data"][col])
            columns.append(list(zip(*temp)))
        joined_data = list(itertools.product(*columns))
        for obj in joined_data:
            row = []
            for data in obj:
                row.extend(list(data))
            for i, col in enumerate(self.query_data["columns"]):
                self.query_data["data"][col].append(row[i])

    def check_column(self, column):
        data_columns = [col.split(".")[1] for col in self.query_data["columns"]]
        if "." in column:
            self.handle_error(
                column in self.query_data["columns"], "Incorrect column name provided"
            )
            col = column
        else:
            self.handle_error(column in data_columns, "Incorrect column name provided")
            self.handle_error(
                data_columns.count(column) == 1, "Ambiguous column name given"
            )
            col = self.query_data["columns"][data_columns.index(column)]
        return col

    def get_matching_indices(self, cond):
        ret = []
        colname = self.check_column(cond[0])
        col1 = self.query_data["data"][colname]
        if not self.is_int(cond[2]):
            colname = self.check_column(cond[2])
            col2 = self.query_data["data"][colname]
            for i, val in enumerate(zip(col1, col2)):
                if self.operators[cond[1]](val[0], val[1]):
                    ret.append(i)
        else:
            for i, val in enumerate(col1):
                if self.operators[cond[1]](val, int(cond[2])):
                    ret.append(i)
        return ret

    def execute_conditions(self):
        if self.query_conditions:
            filteredInd = []
            ind1 = self.get_matching_indices(self.query_conditions[0])
            if self.condOp:
                ind2 = self.get_matching_indices(self.query_conditions[1])
                filteredInd = self.operators[self.condOp](ind1,ind2)
            else:
                filteredInd = ind1
            for col in self.query_data["columns"]:
                self.query_data["data"][col] = [self.query_data["data"][col][i] for i in filteredInd]

    def execute_aggregation(self, col, table):
        ret = 0
        data = []
        col = self.check_column(col)
        data = self.query_data["data"][col]
        if self.aggregationOp == "max":
            ret = max(data)
        elif self.aggregationOp == "sum":
            ret = sum(data)
        elif self.aggregationOp == "min":
            ret = min(data)
        else:
            ret = sum(data) / len(data)
        print(self.aggregationOp + "(" + col + ")")
        print(ret)

    def project_columns(self):
        if self.query_columns[0] != "*":
            for col in self.query_columns:
                column_name = self.check_column(col)
                self.projected_data["columns"].append(column_name)
                self.projected_data["data"][column_name] = self.query_data["data"][
                    column_name
                ]
        else:
            self.projected_data = self.query_data

    def display_table(self):
        header = []
        data = []
        rows = []
        for col in self.projected_data["columns"]:
            header.append(col)
            data.append(self.projected_data["data"][col])
        print(",".join(header))
        for i in range(len(data[0])):
            row = []
            for j in range(len(header)):
                row.append(str(data[j][i]))
            if self.distinctOp:
                if row not in rows:
                    print(",".join(row))
                    rows.append(row)
            else:
                print(",".join(row))
                rows.append(row)

    def read_table(self, table_metadata):
        tablename = table_metadata[0]
        filename = table_metadata[0] + ".csv"
        columns = table_metadata[1:]
        self.tables[tablename] = {"columns": columns, "data": {}}
        self.tables[tablename]["data"] = dict(
            zip(columns, [[] for i in range(len(columns))])
        )
        with open(os.path.join(self.DATA_FOLDER, filename)) as f:
            for line in f.readlines():
                values = (line.strip("\n")).split(",")
                for ind, col in enumerate(columns):
                    self.tables[tablename]["data"][col].append(
                        int(re.sub("['\"]", "", values[ind]))
                    )

    def read_metadata(self):
        metadata = []
        with open(os.path.join(self.DATA_FOLDER, "metadata.txt")) as f:
            metadata = [line.strip("\n") for line in f.readlines()]

        end_indices = [i for i, x in enumerate(metadata) if x == "<end_table>"]
        start_indices = [i for i, x in enumerate(metadata) if x == "<begin_table>"]
        for start, end in zip(start_indices, end_indices):
            self.read_table(metadata[start + 1 : end])

    @staticmethod
    def handle_error(cond=False, msg="Incorrect query format"):
        if not cond:
            sys.exit("[SQL-ENGINE]: {}".format(msg))

    @staticmethod
    def is_int(s):
        try:
            int(s)
            return True
        except ValueError:
            return False


if __name__ == "__main__":
    if len(sys.argv) == 2:
        SQLEngine.execute_query(sys.argv[1])
    else:
        SQLEngine.handle_error()
