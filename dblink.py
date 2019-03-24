import struct
import sys
import os
import time
import json

version = 1
autoid = int

class Database():
    def __init__(self, directory):
        self.directory = directory
        self.tables = []
        self.config = os.path.join(directory, "config")
        if not os.path.exists(directory):
            os.mkdir(directory)
        self.autoid = Table("autoid")
        self.autoid.add_field('table', str)
        self.autoid.add_field('value', int)
        self.add(self.autoid)

    def next_autoid(self, table):
        id = self.autoid.one(table=table)
        self.autoid.delete()
        v = [table, 0]
        if isinstance(id, dict):
            v = [id["table"], id["value"]+1]
        self.autoid.dump([v])
        return v[-1]

    def save(self):
        pass
    
    def add(self, table):
        table._conn = self
        path = os.path.join(self.directory, table.filename)
        if not os.path.exists(path):
            f = open(path, "wb")
            f.write(table.get_headers())
            f.close()
        table.filename = path
        self.tables.append(table)    

class Table():
    def __init__(self, name):
        self.name = name
        self.filename = name+".dbt"
        self.column_names = []
        self.column_types = []
        self.defaults = {}
        self._conn = None
    
    def add_field(self, name, typ, default=None):
        if name in self.column_names:
            raise Exception('Field already exists!')
        self.column_names.append(name)
        self.column_types.append(typ)
        self.defaults[name] = default

    def dump(self, vals):
        self._clens = len(self.column_names)
        if os.path.exists(self.filename):
            f = open(self.filename, "ab")
            saved_headers = open(self.filename, "rb").readline()
            actual_headers = self.get_headers()
            if saved_headers != actual_headers:
                for n in self.load():
                    row_vals = []
                    for s in self.column_names:
                        if s not in n:
                            n[s] = self.defaults[s]
                        row_vals.append(n[s])
                    f.write(self._row(row_vals)+b";")
                h = open(self.filename, "wb")
                h.write(actual_headers)
                h.close()
        else:
            f = open(self.filename, "wb")
            f.write(self.get_headers())
        for v in vals:
            f.write(self._row(v)+b";")
        f.close()
    
    def _row(self, row):
        b = b""
        for r in row:
            if isinstance(r, int):
                b += struct.pack("i", r)
            elif isinstance(r, float):
                b += struct.pack("f", r)
            elif isinstance(r, (list, dict)):
                r = json.dumps(r)
                b += struct.pack("i", len(r)) + r.encode()
            elif r == None:
                b += b"null"
            else:
                b += struct.pack("i", len(r)) + r.encode()
        rlen = len(row)
        if rlen < self._clens:
            b += b"null"*(self._clens-rlen)
        return b
    
    def get_headers(self):
        return " ".join(["%s:%s" % (s, self.column_types[self.column_names.index(s)].__name__) for s in self.column_names]).encode()+b"\n"
    
    def load(self):
        if not os.path.exists(self.filename):
            f = open(self.filename, "wb")
            f.write(self.get_headers())
            f.close()
            return {}
        f = open(self.filename, "rb")
        headers = f.readline().decode().split()
        line = ";"
        while line.strip() != "":
            self._last_pos = f.tell()
            row = {}
            for s in headers:
                field, type = s.split(":")
                if type == "int":
                    i = f.read(4)
                    if not i: return
                    if i == b"null":
                        row[field] = None
                    else:
                        row[field] = struct.unpack("i", i)[0]
                elif type == "float":
                    i = f.read(4)
                    if not i: return
                    if i == b"null":
                        row[field] = None
                    else:
                        row[field] = struct.unpack("f", i)[0]
                elif type == "str":
                    i = f.read(4)
                    if not i: return
                    if i == b"null":
                        row[field] = None
                    else:
                        length = struct.unpack("i", i)[0]
                        row[field] = f.read(length).decode()
                elif type == "list" or type == "dict":
                    i = f.read(4)
                    if not i: return
                    if i == b"null":
                        row[field] = None
                    else:
                        length = struct.unpack("i", i)[0]
                        row[field] = json.loads(f.read(length).decode())
            line = f.read(1)
            yield row
        f.close()
    
    def delete(self):
        if os.path.exists(self.filename):
            f = open(self.filename, "wb")
            f.write(self.get_headers())
            f.close()
    
    def all(self, **kwargs):
        for r in self.load():
            for k in kwargs:
                if k in r and r[k] == kwargs[k]:
                    yield r
    
    def one(self, **kwargs):
        for r in self.load():
            for k in kwargs:
                if k in r and r[k] == kwargs[k]:
                    return r
    
    def pop(self, one=False, **kwargs):
        delete = False
        ld = self.load()
        last = None
        for r in ld:
            for k in kwargs:
                if last:
                    self._remove_range(last, self._last_pos)
                if k in r and r[k] == kwargs[k]:
                    last = self._last_pos
        if last:
            self._remove_range(last, self._last_pos)
    
    def _remove_range(self, start, stop):
        f2 = open(self.filename+".tmp", "wb")
        f = open(self.filename, 'rb+')
        #print(stop, "-", start, "=", stop-start)
        f2.write(f.read(start))
        #print(f.read(stop-start))
        f.seek(stop)
        f2.write(f.read())
        f.close()
        f2.close()
        os.rename(self.filename+".tmp", self.filename)
    
    def count(self):
        return sum(1 for _ in self.load())
    
    @property
    def autoid(self):
        return self._conn.next_autoid(self.name)