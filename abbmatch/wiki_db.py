import sqlite3


class WikiDB:
    def __init__(self, db_name='abbmatch.db'):
        self.db_name = db_name
        self._connect()

    def _connect(self):
        self.conn = sqlite3.connect(self.db_name)

    def close_conn(self):
        self.conn.close()
        self.conn = None

    def create_database(self):
        c = self.conn.cursor()
        # Create tables
        c.execute("""
            CREATE TABLE IF NOT EXISTS link_map (
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                depth INTEGER NOT NULL,
                parent_id INTEGER, link TEXT UNIQUE
            )""")

        # Create table
        c.execute("""
            CREATE TABLE IF NOT EXISTS content (
                id_link INTEGER, content TEXT,
                title TEXT
            )""")
        self.conn.commit()

    def _prepare_value_sql(self, v):
        v = str(v).replace("\"", "'")
        return F"\"{v}\""

    def insert_many(self, table, col=None, value_placeholder=None,
                    values=None, or_ignore=True):
        assert isinstance(value_placeholder, (list, tuple))

        c = self.conn.cursor()
        or_ignore = 'OR IGNORE' if or_ignore else ''
        value_placeholder = [F"\"{v}\"" if v is not None else '?' for v in value_placeholder]
        value_placeholder = F"({','.join(value_placeholder)})"
        col = col if col is None else F"({','.join(col)})"
        sql = F"INSERT {or_ignore} INTO {table} {col} VALUES {value_placeholder}"
        c.executemany(sql, values)
        # Save (commit) the changes
        self.conn.commit()

    def insert_one(self, table, col=None, values=None, or_ignore=True):
        assert values is not None

        c = self.conn.cursor()
        or_ignore = 'OR IGNORE' if or_ignore else ''
        col = '' if col is None else F"({','.join(col)})"
        values = [self._prepare_value_sql(v) for v in values]
        values = F"({','.join(values)})"
        sql = F"INSERT {or_ignore} INTO {table} {col} VALUES {values}"
        # print(sql)
        try:
            c.execute(sql)
        except Exception:
            raise Exception(sql)
        # Save (commit) the changes
        self.conn.commit()

    def _fetchall(self, sql):
        c = self.conn.cursor()
        c.execute(sql)
        return c.fetchall()

    # def store_links(self, parent_id, link_list):
    #     sql = F"INSERT INTO link_map (parent_id, link) VALUES ({parent_id}, ?)"
    #     link_list = [(l,) for l in link_list]
    #     self._insert_many(sql, link_list)

    def select(self, col, table, where=None, limit=10):
        col = col if isinstance(col, str) else ','.join(col)
        where = F"where {where}" if where is not None else ""
        sql = F"SELECT {col} FROM {table} {where} limit {limit}"
        return self._fetchall(sql)

    def get_unvisited_links(self, limit=30):
        sql = F"""
            SELECT id, link FROM link_map as A
            LEFT JOIN content as B ON A.id=B.id_link
            WHERE B.content is NULL ORDER BY ID
            LIMIT {limit}
        """
        return self._fetchall(sql)



    # Insert the starting point
    # c.execute(f"INSERT OR IGNORE INTO link_map VALUES ('1', '-1', '{starting_point}')")
