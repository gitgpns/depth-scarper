import psycopg2


class SaverDB:
    table_name = 'depth'
    dbname = 'oleg'
    user = 'oleg'
    fields = 'exchange, extra'

    def __init__(self, exchange):
        self.exchange = exchange
        self._conn = psycopg2.connect(f"dbname={self.dbname} user={self.user}")
        self._cur = self._conn.cursor()

    def save_data(self, data):
        query = f"INSERT INTO {self.table_name} ({self.fields}) VALUES ('{self.exchange}', '{data}')"

        self._cur.execute(query)
        self._conn.commit()

        print(f"Executed: {query}")
