from sshtunnel import SSHTunnelForwarder
import os
import psycopg2
from psycopg2.extras import RealDictCursor

class Database:
    def __init__(self, use_ssh: bool, dsn_connection_str: str):
        self.use_ssh = use_ssh
        self.dsn_connection_str = dsn_connection_str
        if self.use_ssh:
            # connect with ssh
            ssh_host = os.getenv("SSH_HOST")
            ssh_port = int(os.getenv("SSH_PORT", ""))
            ssh_user = os.getenv("SSH_USER")
            ssh_pkey = os.getenv("SSH_PKEY")
            ssh_rbind_addr = os.getenv("SSH_RBIND_ADDR")
            ssh_rbind_port = int(os.getenv("SSH_RBIND_PORT", ""))
            self.server = SSHTunnelForwarder((ssh_host, ssh_port),
                     ssh_username=ssh_user,
                     ssh_pkey=ssh_pkey,
                     remote_bind_address=(ssh_rbind_addr, ssh_rbind_port)
                     )
            self.server.start()
            self.conn = psycopg2.connect(
                self.dsn_connection_str,
                host=self.server.local_bind_host,
                port=self.server.local_bind_port
                )
            if self.conn.closed == 0:
                print("successfully connected to database ...")


        else:
            # connect as normal
            self.conn = psycopg2.connect(self.dsn_connection_str)
            if self.conn.closed == 0:
                print("successfully connected to database ...")

        self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)



    def commit(self):
        self.conn.commit()

    def close(self, commit=True):
        if commit:
            self.conn.commit()
        if self.use_ssh:
            self.conn.close()
            self.server.close()
            print("closed database and server ...")
        else:
            self.conn.close()
            print("closed database ...")

    def execute(self, sql, params=None):
        self.cursor.execute(sql, params or ())
        return self

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()

    def query(self, sql, params=None):
        self.cursor.execute(sql, params or ())
        return self.cursor.fetchall()
