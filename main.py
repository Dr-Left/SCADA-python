import sys
import time

from PyQt5.QtWidgets import QApplication, QMainWindow, QTableWidgetItem
from sqlalchemy import create_engine

import database_util
from SCADA_UI.EMS_UI import Ui_MainWindow


class MyMainWnd(QMainWindow):
    def __init__(self):
        super(MyMainWnd, self).__init__()
        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)
        self.engine = create_engine("sqlite:///db/ems.db")
        self.update_from_sql()
        self.cbx_filter()
        self.ui.editFilter.textChanged.connect(self.update_from_sql)
        self.ui.btnRefresh.clicked.connect(self.update_from_sql)
        self.ui.cbxRtu.currentIndexChanged.connect(self.cbx_filter)

    def update_from_sql(self):
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
                        widget.setItem(row, col,
                            QTableWidgetItem(str(results[row][col]))
                        )

    def cbx_filter(self):
        cbx = self.ui.cbxRtu
        self.ui.editFilter.setText(self.ui.cbxRtu.currentText()[-4:])
        self.ui.labelRtuName = cbx.currentText()[3:] if cbx.currentText() != "rtu" else "/"
        self.ui.labelRtuID = cbx.currentText()[0] if cbx.currentText() != "rtu" else "/"
        # self.ui.labelRtuPort = cbx

if __name__ == "__main__":
    app = QApplication(sys.argv)
    wnd = MyMainWnd()
    wnd.show()
    sys.exit(app.exec())
