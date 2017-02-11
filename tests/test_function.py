import pytest
import base64
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


def SetB64EncodeFunc(conn_obj):
    def __helper(bytes_obj):
        if isinstance(bytes_obj, unicode):
            temp = bytes_obj.encode('UTF-8')
        else:
            temp = bytes_obj if bytes_obj is not None else ''
        return base64.b64encode(temp)
    conn_obj.create_det_function('b64encode', 1, __helper)


@pytest.mark.skipif(False, reason='')
class TestSqlite(object):
    @pytest.mark.skipif(False, reason='')
    def test_det_function(self):
        with ConnWrap(_sqlite3.connect("./bk1.db")) as conn:
            conn.conn_obj.execute("drop table if exists test_det_func")

            conn.conn_obj.execute("create table if not exists test_det_func(id text, val integer)")
            SetB64EncodeFunc(conn.conn_obj)
            conn.conn_obj.execute("create index test_det_func_idx1 on test_det_func(b64encode(id))")

        with ConnWrap(_sqlite3.connect("./bk1.db")) as conn:
            SetB64EncodeFunc(conn.conn_obj)
            for i in xrange(1000):
                conn.conn_obj.execute("insert into test_det_func(id,val) values (?,?)", (unicode(i), i+1))

        with ConnWrap(_sqlite3.connect("./bk1.db")) as conn:
            SetB64EncodeFunc(conn.conn_obj)
            res = conn.conn_obj.execute_query_plan("select * from test_det_func where b64encode(id)=?", ('1',))
            rows = [row for row in res if row[3].find(u'USING INDEX') > 0 and row[3].find(u'<expr>') > 0]
            assert len(rows) == 1


