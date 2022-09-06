import datetime
import functools
import sys
import time
import traceback

from PyQt5.QtWidgets import QApplication, QMainWindow, QTableWidgetItem, QHeaderView
from PyQt5.QtCore import QTimer
from sqlalchemy import create_engine

import database_util
from SCADA_UI.EMS_UI import Ui_MainWindow


def catch_except(func):
    def inner(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as err:
            traceback.print_exc()
    return inner


def disable_listener(func):
    def inner(self, *args, **kwargs):
        # print(self.ui.tableYk.cellChanged.isSignalConnected(self.func1))
        # if self.ui.tableYk.cellChanged.isSignalConnected(self.func1):
        self.ui.tableYk.cellChanged.disconnect(self.func1)
        self.ui.tableYt.cellChanged.disconnect(self.func2)
        ret = func(self, *args, **kwargs)
        self.ui.tableYk.cellChanged.connect(self.func1)  # elegant
        self.ui.tableYt.cellChanged.connect(self.func2)  # elegant
        return ret
    return inner


class MyMainWnd(QMainWindow):

    def __init__(self):
        super(MyMainWnd, self).__init__()
        self.func1 = functools.partial(self.update_db, type_="yk")
        self.func2 = functools.partial(self.update_db, type_="yt")
        self.timer = QTimer()
        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)
        self.engine = create_engine("sqlite:///db/ems.db")
        self.ui.tableYk.cellChanged.connect(self.func1)  # elegant
        self.ui.tableYt.cellChanged.connect(self.func2)  # elegant
        self.update_from_sql()
        self.cbx_filter()
        self.ui.editFilter.textChanged.connect(self.update_from_sql)
        self.ui.btnRefresh.clicked.connect(self.update_from_sql)
        self.ui.cbxRtu.currentIndexChanged.connect(self.cbx_filter)
        self.ui.btnEnableAutoRefresh.clicked.connect(self.enable_auto_refresh)
        for x in [self.ui.tableYc, self.ui.tableYx, self.ui.tableYk, self.ui.tableYt]:
            x.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
            x.horizontalHeader().setSectionResizeMode(5, QHeaderView.ResizeToContents)
        self.timer.timeout.connect(self.update_from_sql)
        self.show()

    @catch_except
    @disable_listener
    def update_from_sql(self, *args):
        with self.engine.connect() as conn:
            if self.ui.cbxRtu.count() == 0:
                results = conn.execute("select id, name from ems_rtu_info").fetchall()
                self.ui.cbxRtu.addItem("rtu")
                for id, name in results:
                    # print(id, name)
                    self.ui.cbxRtu.addItem(f"{id} {name}")
            for i, widget, operation in [
                (1, self.ui.tableYc, "yc"),
                (2, self.ui.tableYx, "yx"),
                (3, self.ui.tableYt, "yt"),
                (4, self.ui.tableYk, "yk"),
            ]:
                results = (
                    conn.execute("select * from ems_{0}_info where name like '%{1}%' order by name"
                    .format(operation, self.ui.editFilter.text()))
                    .fetchall())
                widget.setRowCount(len(results))
                columns = database_util.ems_tables[i]["columns"]
                # print(columns)
                widget.setColumnCount(len(columns))
                widget.setHorizontalHeaderLabels(columns)
                for row, _ in enumerate(results):
                    for col in range(len(columns)):
                        result = results[row][col]
                        if columns[col] == "refresh_time":
                            result = datetime.datetime.fromtimestamp(result)
                        result = str(result)
                        widget.setItem(row, col,
                            QTableWidgetItem(result)
                        )

    def query_rtu_info(self, rtu_id):
        with self.engine.connect() as conn:
            results = conn.execute("select name, ip_addr, port, status, refresh_time from ems_rtu_info where id = {0}"
                                   .format(rtu_id)).fetchall()
            for name, ip_addr, port, status, refresh_time in results:
                return [name, ip_addr,str(port), str(status), refresh_time]
            return None

    def cbx_filter(self):
        cbx = self.ui.cbxRtu
        if cbx.currentIndex() > 0:
            try:
                rtu_id = int(cbx.currentText()[0])
                rtu_info = self.query_rtu_info(rtu_id)
                self.ui.editFilter.setText(self.ui.cbxRtu.currentText()[-4:])
                self.ui.labelRtuName.setText("\n".join(["RTU Name", rtu_info[0]]))
                self.ui.labelRtuID.setText("\n".join(["RTU ID", str(rtu_id)]))
                self.ui.labelRtuStatus.setText("\n".join(["RTU Status", rtu_info[3]]))
                self.ui.labelRtuPort.setText("\n".join(["RTU Port"]+rtu_info[1:3]))
                date = datetime.datetime.fromtimestamp(rtu_info[4])
                self.ui.labelRtuRefresh_time.setText("\n".join(["RTU Refresh Time", str(date)]))
            except Exception as e:
                print(e)
                traceback.print_exc()
            # self.ui.labelRtuPort = cbx
        else:
            self.ui.editFilter.setText(self.ui.cbxRtu.currentText())
            self.ui.labelRtuName.setText("RTU Name\n")
            self.ui.labelRtuID.setText("RTU ID\n")
            self.ui.labelRtuStatus.setText("RTU Status\n")
            self.ui.labelRtuPort.setText("RTU Port\n")
            self.ui.labelRtuRefresh_time.setText("RTU Refresh Time\n")

    @catch_except
    def update_db(self, row, col, type_):
        if type_ == "yk":
            table = self.ui.tableYk
        elif type_ == "yt":
            table = self.ui.tableYt

        column_header = table.horizontalHeaderItem(col).text()
        value = table.item(row, col).text()
        rtu_id = int(table.item(row, 0).text())
        pnt_no = int(table.item(row, 1).text())
        with self.engine.connect() as conn:
            conn.execute("update ems_{0}_info set {1}={2} where rtu_id={3} and pnt_no={4}"
                         .format(type_, column_header, value, rtu_id, pnt_no))

            conn.execute("update ems_{0}_info set refresh_time='{1}' where rtu_id={2} and pnt_no={3}"
                         .format(type_, int(time.time()), rtu_id, pnt_no))
        self.update_from_sql()

    def enable_auto_refresh(self):
        if not self.timer.isActive():
            self.timer.start(1000)
            self.ui.btnEnableAutoRefresh.setText("停用自动刷新")
            self.ui.labelAutoRefreshPeriod.setText("自动刷新周期: 1000ms")
        else:
            self.timer.stop()
            self.ui.btnEnableAutoRefresh.setText("启用自动刷新")
            self.ui.labelAutoRefreshPeriod.setText("自动刷新周期: 已禁用")



if __name__ == "__main__":
    app = QApplication(sys.argv)
    wnd = MyMainWnd()
    sys.exit(app.exec())
