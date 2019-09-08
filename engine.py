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
        self.aggregationOp = None
        self.distinctOp = False
        self.query_tables = []
        self.query_conditions = []
        self.query_data = {"columns": [], "data": {}}
        self.projected_data = {"columns": [], "data": {}}

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
            self.query_conditions = tokens[tokens.index("where") + 1 :]
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
            if re.match("^(sum|max|avg)(\([\w*.-]+\))$", columns[0]):
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
            all(table in self.tables for table in self.query_tables), "Table not found"
        )
        return tables

    def run_query(self):
        if self.aggregationOp is not None:
            self.execute_aggregation(self.query_columns[0], self.query_tables[0])
        else:
            self.join_tables()
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

    def execute_aggregation(self, col, table):
        ret = 0
        data = []
        if "." in col:
            self.handle_error(table == col.split(".")[0], "Ambiguous column name given")
            col = col.split(".")[1]
        self.handle_error(
            col in self.tables[table]["data"], "column not present in table"
        )
        data = self.tables[table]["data"][col]
        if self.aggregationOp == "max":
            ret = max(data)
        elif self.aggregationOp == "sum":
            ret = sum(data)
        else:
            ret = sum(data) / max(data)
        print(self.aggregationOp + "(" + table + "." + col + ")")
        print(ret)

    def project_columns(self):
        if self.query_columns[0] != "*":
            data_columns = [col.split(".")[1] for col in self.query_data["columns"]]
            for col in self.query_columns:
                if "." in col:
                    self.handle_error(
                        col in self.query_data["columns"],
                        "Incorrect column name provided",
                    )
                    self.projected_data["columns"].append(col)
                    self.projected_data["data"][col] = self.query_data["data"][col]
                else:
                    self.handle_error(
                        col in data_columns, "Incorrect column name provided"
                    )
                    self.handle_error(
                        data_columns.count(col) == 1, "Ambiguous column name given"
                    )
                    column_name = self.query_data["columns"][data_columns.index(col)]
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
        print("length of result table: {}".format(len(rows)))

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
    def handle_error(cond, msg="Incorrect query format"):
        if not cond:
            sys.exit("[SQL-ENGINE]: {}".format(msg))


if __name__ == "__main__":
    if len(sys.argv) == 2:
        SQLEngine.execute_query(sys.argv[1])
    else:
        SQLEngine.handle_error()
