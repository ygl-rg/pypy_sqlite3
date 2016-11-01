import pytest
import src._sqlite3 as _sqlite3


class ConnWrap:
    def __init__(self, conn_obj):
        self.conn_obj = conn_obj

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None and exc_val is None and exc_tb is None:
            self.conn_obj.commit()
        else:
            self.conn_obj.rollback()
        self.conn_obj.close()


@pytest.mark.skipif(False, reason='')
class TestSqlite(object):
    @pytest.mark.skipif(False, reason='')
    def test_backup(self):
        with ConnWrap(_sqlite3.connect("./bk1.db")) as conn:
            conn.conn_obj.execute("drop table if exists test")
            conn.conn_obj.execute("create table if not exists test(id integer, val integer)")
        with ConnWrap(_sqlite3.connect("./bk1.db")) as conn:
            for i in xrange(1000):
                conn.conn_obj.execute("insert into test(id,val) values (?,?)", (i, i+1))
        with ConnWrap(_sqlite3.connect("./bk1.db")) as conn:
            with ConnWrap(_sqlite3.connect("./bk2.db")) as conn2:
                conn.conn_obj.backup(conn2.conn_obj)

        with ConnWrap(_sqlite3.connect("./bk2.db")) as conn2:
            rows = list(conn2.conn_obj.execute("select count(1) from test"))
            assert rows[0][0] == 1000

