import sqlite3
from sqlite3 import Connection
from typing import List, Union

from track_historical_staked_sui import StakedSuiRef, SuiCoinRef, DeletedObjectRef

class SqliteManager:
    def __init__(self, version="v1", purge=True):
        if version == "v1":
            self.init_v1(purge)
        else:
            self.init_v2(purge)

    def init_v2(self, purge=True):
        self.conn = sqlite3.connect("sui_data.db", check_same_thread=False)
        cursor = self.conn.cursor()

        if purge:
            cursor.execute("DROP TABLE IF EXISTS staked_sui_v2")
            cursor.execute("DROP TABLE IF EXISTS sui_coins_v2")
        self.conn.commit()            

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS staked_sui_v2 (
                object_id TEXT NOT NULL,               
                version INTEGER NOT NULL,
                at_epoch INTEGER NOT NULL,
                owner TEXT NOT NULL,
                pool_id TEXT,
                principal INTEGER,
                stake_activation_epoch INTEGER,     
                deleted BOOLEAN NOT NULL,           
                PRIMARY KEY (object_id, version)

        )
        """)
        self.conn.commit()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS sui_coins_v2 (
                object_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                at_epoch INTEGER NOT NULL,
                owner TEXT NOT NULL,
                balance INTEGER,
                deleted BOOLEAN NOT NULL,
                PRIMARY KEY (object_id, version)
        )
        """)
        self.conn.commit()
        cursor.close()

    def init_v1(self, purge=True):
        self.conn = sqlite3.connect("sui_data.db", check_same_thread=False)
        cursor = self.conn.cursor()

        if purge:
            cursor.execute("DROP TABLE IF EXISTS staked_sui")
            cursor.execute("DROP TABLE IF EXISTS sui_coins")
        self.conn.commit()            

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS staked_sui (
                object_id TEXT NOT NULL,                
                version INTEGER NOT NULL,
                owner TEXT NOT NULL,
                pool_id TEXT NOT NULL,
                principal INTEGER NOT NULL,
                stake_activation_epoch INTEGER NOT NULL,
                at_epoch INTEGER NOT NULL,
                PRIMARY KEY (object_id, at_epoch)

        )
        """)
        self.conn.commit()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS sui_coins (
                object_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                owner TEXT NOT NULL,
                balance INTEGER NOT NULL,
                at_epoch INTEGER NOT NULL,
                PRIMARY KEY (object_id, at_epoch)
        )
        """)
        self.conn.commit()
        cursor.close()

    def insert_batch_staked_sui_v2(self, items: List[Union[StakedSuiRef, DeletedObjectRef]]):
        cursor = self.conn.cursor()
        data = []
        for item in items:
            if isinstance(item, StakedSuiRef):
                data.append((item.object_id, item.version, item.at_epoch, item.owner, item.pool_id, item.principal, item.stake_activation_epoch, False))
            else:
                data.append((item.object_id, item.version, item.at_epoch, item.owner, None, None, None, True))        

        cursor.executemany("""
            INSERT OR REPLACE INTO staked_sui_v2 (object_id, version, at_epoch, owner, pool_id, principal, stake_activation_epoch, deleted)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, data)

        self.conn.commit()
        cursor.close()

    def insert_batch_sui_coin_v2(self, items: List[Union[SuiCoinRef, DeletedObjectRef]]):
        cursor = self.conn.cursor()
        data = []
        for item in items:
            if isinstance(item, SuiCoinRef):
                data.append((item.object_id, item.version, item.at_epoch, item.owner, item.balance, False))
            else:
                data.append((item.object_id, item.version, item.at_epoch, item.owner, None, True))

        cursor.executemany("""
            INSERT OR REPLACE INTO sui_coins_v2 (object_id, version, at_epoch, owner, balance, deleted)
            VALUES (?, ?, ?, ?, ?, ?)
        """, data)

        self.conn.commit()
        cursor.close()

    def insert_batch_staked_sui(self, items: List[StakedSuiRef]): 
        cursor = self.conn.cursor()            
        data = [(item.object_id, item.version, item.owner, item.pool_id, item.principal, item.stake_activation_epoch, item.at_epoch) for item in items]

        cursor.executemany("""
            INSERT OR REPLACE INTO staked_sui (object_id, version, owner, pool_id, principal, stake_activation_epoch, at_epoch)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, data)

        self.conn.commit()
        cursor.close()

    def insert_batch_sui_coin(self, items: List[SuiCoinRef]):
        cursor = self.conn.cursor()
        data = [(item.object_id, item.version, item.owner, item.balance, item.at_epoch) for item in items]

        cursor.executemany("""
            INSERT OR REPLACE INTO sui_coins (object_id, version, owner, balance, at_epoch)
            VALUES (?, ?, ?, ?, ?)
        """, data)

        self.conn.commit()
        cursor.close()
