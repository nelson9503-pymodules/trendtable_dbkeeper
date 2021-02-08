from . import mysqlite
from . import trendtable
from datetime import datetime


class DBKeeper:

    def __init__(self, folder_path: str):
        """
        Trend Table Database Keeper manage the trend table data.
        Keeper will save the .db in the folder_path.
        """
        self.db_path = folder_path + "/trendtable.db"
        self.__initialize()

    def update(self, symbol: str, price_data: dict, skipUpdated: bool = True):
        """
        Calculate the trend table values and
        update the trend table data to database.
        If skipUpdated is True, keeper will skip the symbol had been updated today.
        """
        # create table if not exists
        if not symbol in self.mastertb:
            # duoble check
            self.mastertb = self.master.query()
            if not symbol in self.mastertb:
                self.__create_trendtable_table(symbol)
        self.tb = self.db.TB(symbol)
        # skip if updated
        today = self.__get_today()
        self.config = self.master.query(
            "*", 'WHERE table_name = "{}"'.format(symbol))
        lastupdate = self.config[symbol]["last_update"]
        if skipUpdated == True and today == lastupdate:
            return
        # skip if data points less than 378
        if len(price_data) < 378:
            return
        # update
        self.config = self.master.query(
            "*", 'WHERE table_name = "{}"'.format(symbol))
        last_date = self.config[symbol]["last_date"]
        dates = list(price_data.keys())
        dates.sort()
        updates = {}
        n = 378
        printcheck = False
        for i in range(378, len(dates)):
            n += 1
            if dates[i] < last_date:
                continue
            printcheck = True
            print("dbkeeper: calculating trend table: {}/{}".format(n, len(dates)), end="\r")
            slice_dates = dates[i-378:i]
            price_list = []
            for date in slice_dates:
                adjclose = price_data[date]["adjclose"]
                if adjclose <= 0 or adjclose == None:
                    price_list = False
                    break
                price_list.append(adjclose)
            trend_vals = trendtable.cal_trend_serious(price_list, 3)
            update = {}
            for interval in trend_vals:
                update["tb"+str(interval)] = trend_vals[interval]
            updates[dates[i]] = update
        if printcheck == True:
            print()
        self.tb.update(updates)
        # update lastupdate
        self.config[symbol]["last_update"] = today
        if self.config[symbol]["first_date"] == 0 and len(dates) >= 378:
            self.config[symbol]["first_date"] = dates[377]
        if len(dates) >= 378:
            self.config[symbol]["last_date"] = max(dates)
        if self.config[symbol]["data_points"] == 0 and len(price_data) >= 378:
            self.config[symbol]["data_points"] = len(price_data) - 377
        else:
            self.config[symbol]["data_points"] += len(updates)
        self.master.update(self.config)
        self.db.commit()

    def query_trendtable(self, symbol: str, start_time_stamp: int = None, end_time_stamp: int = None) -> dict:
        if not symbol in self.mastertb:
            # duoble check
            self.mastertb = self.master.query()
            if not symbol in self.mastertb:
                return False
        self.tb = self.db.TB(symbol)
        condition = ""
        if not start_time_stamp == None:
            condition += "date >= {}".format(start_time_stamp)
        if not end_time_stamp == None:
            if len(condition) > 0:
                condition += " AND "
            condition += "date <= {}".format(end_time_stamp)
        if len(condition) > 0:
            condition = "WHERE " + condition
        query = self.tb.query("*", condition)
        return query

    def query_master_info(self, symbol: str) -> dict:
        if not symbol in self.mastertb:
            # duoble check
            self.mastertb = self.master.query()
            if not symbol in self.mastertb:
                return False
        info = self.mastertb[symbol]
        return info

    def query_full_master_info(self) -> dict:
        self.mastertb = self.master.query()
        return self.mastertb

    def __initialize(self):
        self.db = mysqlite.DB(self.db_path)
        if not "master" in self.db.listTB():
            self.master = self.db.createTB("master", "table_name", "CHAR(100)")
            self.master.addCol("last_update", "INT")
            self.master.addCol("first_date", "INT")
            self.master.addCol("last_date", "INT")
            self.master.addCol("data_points", "INT")
        else:
            self.master = self.db.TB("master")
        self.mastertb = self.master.query()

    def __create_trendtable_table(self, symbol: str):
        self.tb = self.db.createTB(symbol, "date", "INT")
        for i in range(3, 378+1, 3):
            self.tb.addCol("tb"+str(i), "FLOAT")
        self.master.update({
            symbol: {
                "last_update": 0,
                "first_date": 0,
                "last_date": 0,
                "data_points": 0
            }})
        self.db.commit()

    def __get_today(self) -> int:
        now = datetime.now()
        now = datetime(now.year, now.month, now.day)
        return int(now.timestamp())
