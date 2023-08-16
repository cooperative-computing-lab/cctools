import numbers
import time
import sys
import math
from datetime import timedelta
from string import Template


class StatusDisplay:
    resources = ["cores", "gpus", "memory", "disk"]

    def __init__(self, interval=10):
        self.interval = interval
        self._last_update_report = 0

    def active(self):
        raise NotImplementedError

    def update(self, queue=None, force=False):
        now = time.time()
        if not force:
            if self._last_update_report + self.interval > now:
                return
        self._last_update_report = now

        (q_info, rw_info, rc_info, app_info) = self.generate_table_data(queue)
        self.update_display(q_info, rw_info, rc_info, app_info)

    def update_display(self, queue_info, resources_worker_info, resources_category_info, app_info):
        raise NotImplementedError

    def generate_table_data(self, queue=None):
        queue_s = None
        app_s = None
        if queue:
            try:
                queue_s = queue.status("queue")[0]
            except Exception as e:
                print("Error reading work queue status: {}".format(e), file=sys.stderr)
            try:
                app_s = queue.application_info()
            except Exception as e:
                print("Error reading application information status: {}".format(e), file=sys.stderr)

        rc_info = self.generate_resource_category_tables(queue_s)
        rw_info = self.generate_resource_worker_tables(queue_s)
        q_info = self.generate_queue_table(queue_s)
        app_info = self.generate_app_table(app_s)

        return (q_info, rw_info, rc_info, app_info)

    def generate_resource_worker_tables(self, queue_s):
        if not queue_s:
            return None

        suffixes = [("largest", "largest worker"), ("inuse", "allocated"), ("total", "total")]

        rs = []
        header = ["application resources"] + StatusDisplay.resources
        rs.append(header)

        for s, l in suffixes:
            d = self._dict_from_q(queue_s, s)
            rs.append(self.resources_to_row(d, l, "na"))

        return [rs]

    def generate_resource_category_tables(self, queue_s):
        if not queue_s:
            return None

        categories = queue_s["categories"]
        largest_worker = None
        if queue_s["workers_connected"] > 0:
            largest_worker = self._dict_from_q(queue_s, "largest")

        categories.sort(key=lambda c: c["category"])
        cs = []
        for c in categories:
            ct = []
            header = [c["category"]] + StatusDisplay.resources

            ct.append(header)

            larger_worker = self.resources_check_limits(c["max_allocation"], largest_worker)
            if larger_worker:
                ct.append(["current workers are too small!"] + larger_worker)

            allocs = [
                ("max_seen", "largest seen", "na"),
                ("first_allocation", "current allocation", "whole worker"),
                ("max_allocation", "maximum allocation", "whole worker"),
            ]

            for k, l, na in allocs:
                try:
                    alloc = self.resources_to_row(c[k], l, na)
                    if alloc:
                        ct.append(alloc)
                except KeyError:
                    pass

            cs.append(ct)

        return cs

    def generate_queue_table(self, queue_s):
        if not queue_s:
            return None

        stats = [
            "port",
            "tasks_done",
            "tasks_waiting",
            "tasks_running",
            "tasks_exhausted_attempts",
            "workers_connected",
            "workers_busy",
        ]

        pairs = list((key.replace("_", " "), str(queue_s[key])) for key in stats)

        pairs.append(("sent", self.with_units("disk", queue_s["bytes_sent"] / 1e6, "na")))
        pairs.append(("received", self.with_units("disk", queue_s["bytes_received"] / 1e6, "na")))

        pairs.append(("total send time", str(timedelta(seconds=math.ceil(queue_s["time_send"] / 1e6)))))
        pairs.append(("total receive time", str(timedelta(seconds=math.ceil(queue_s["time_send"] / 1e6)))))
        pairs.append(("total good task time", str(timedelta(seconds=math.ceil(queue_s["time_workers_execute_good"] / 1e6)))))
        pairs.append(("total task time", str(timedelta(seconds=math.ceil(queue_s["time_workers_execute"] / 1e6)))))
        pairs.append(("runtime", str(timedelta(seconds=math.ceil(time.time() - queue_s["time_when_started"] / 1e6)))))

        return pairs

    def generate_app_table(self, app_s):
        if not app_s:
            return None

        try:
            app_info = app_s["application_info"]
            values = app_info["values"]
            units = app_info.get("units", {})
        except KeyError:
            return None

        pairs = []
        try:
            for key in values:
                name = key.replace("_", " ")
                unit = units.get(key, None)
                value = values[key]
                if unit == "MB":
                    value = self.with_units("disk", value, "na")
                elif unit:
                    value = f"{value} {unit}"
                else:
                    value = str(value)
                pairs.append((name, value))
        except Exception as e:
            pairs = [("internal error reading status", str(e))]

        return pairs

    def resources_to_row(self, values, alloc, na="na"):
        rs = [alloc]
        for r in StatusDisplay.resources:
            try:
                value = values[r]
                value_with_units = self.with_units(r, value, na)
            except KeyError:
                value_with_units = na
            rs.append(value_with_units)
        return rs

    def resources_check_limits(self, values, limit):
        if not limit:
            return None
        limits_broken = False
        rs = []
        for r in StatusDisplay.resources:
            try:
                largest = limit[r]
                value = values[r]
                check = "ok"
                if isinstance(value, numbers.Number) and isinstance(largest, numbers.Number) and value > largest:
                    limits_broken = True
                    check = f"{self.with_units(r, largest, '')} is too small"
            except KeyError:
                pass
            rs.append(check)
        if limits_broken:
            return rs
        return None

    def with_units(self, resource, value, na):
        if value is None:
            return na

        if not isinstance(value, numbers.Number):
            return str(value)

        if value < 0:
            return na

        if resource not in ["memory", "disk"]:
            return f"{value:.1f}"

        multipliers = ["MB", "GB", "TB"]
        for m in multipliers:
            if value < 1000:
                break
            value /= 1000.0
        return f"{value:.2f} {m}"

    def _dict_from_q(self, queue_s, suffix):
        return {r: queue_s[f"{r}_{suffix}"] for r in StatusDisplay.resources}


class JupyterDisplay(StatusDisplay):
    style = """
        <style>
          table {
            border: 1px solid white;
            border-collapse: collapse;
            width: 100%;
          }
          th {
            background-color: #96D4D4;
            text-align: right;
          }
          td {
            background-color: #f6f8ff;
            padding-right: 15px;
            text-align: right;
          }
          td.over {
            text-align: right;
            background-color: #ffcccc;
          }
        </style>
    """

    tbl_fmt = Template(
        f"""
        <head>
        {style}
        </head>
        <body>
        <table>
        $header
        $rows
        </table>
        </body>
    """
    )

    hdr_fmt = Template('<th colspan="$span"> $value </th>')
    cell_fmt = Template("<td> $value </td>")
    cell_over_fmt = Template('<td class="over"> $value </td>')
    row_fmt = Template("<tr> $cells </tr>")

    def __init__(self, interval=10):
        super().__init__(interval)

        self._output = None
        try:
            import IPython
            import ipywidgets as ws

            if "IPKernelApp" in IPython.get_ipython().config:
                self._queue_display = ws.HTML(value="")
                self._app_display = ws.HTML(value="")
                self._worker_display = ws.HTML(value="")
                self._category_display = ws.HTML(value="")

                self._output = ws.GridspecLayout(1, 8, grid_gap="20px")
                self._output[0, :3] = ws.VBox([self._app_display, self._queue_display])
                self._output[0, 3:] = ws.VBox([self._worker_display, self._category_display])
                IPython.display.display(self._output)

        except (ImportError, AttributeError):
            pass

    def active(self):
        return bool(self._output)

    def update_display(self, queue_info, resources_worker_info, resources_category_info, app_info):
        if app_info:
            self._app_display.value = self.table_of_pairs(app_info, "application info")

        if queue_info:
            self._queue_display.value = self.table_of_pairs(queue_info, "queue stats")

        if resources_worker_info:
            self._worker_display.value = self.table_of_resources(resources_worker_info)

        if resources_category_info:
            self._category_display.value = self.table_of_resources(resources_category_info)

    def table_of_pairs(self, pairs, label):
        header = JupyterDisplay.row_fmt.substitute(cells=JupyterDisplay.hdr_fmt.substitute(value=label, span=2))
        rows = []
        if pairs:
            for pair in pairs:
                row = self.make_row(pair)
                rows.append(row)
        return self.make_table(header, rows)

    def table_of_resources(self, groups):
        rows = []
        for rs in groups:
            rows.append(self.make_row(rs[0], fmt=lambda v: JupyterDisplay.hdr_fmt.substitute(value=v, span=0)))
            rows.extend(map(lambda r: self.make_row(r), rs[1:]))
        return self.make_table("", rows)

    def make_table(self, header, rows):
        return JupyterDisplay.tbl_fmt.substitute(header=header, rows="\n".join(rows))

    def make_row(self, cells_info, fmt=None):
        if not fmt:
            def fmt(v):
                return JupyterDisplay.cell_fmt.substitute(value=v)

            try:
                if cells_info[0][-1] == "!":
                    def fmt(v):
                        return JupyterDisplay.cell_over_fmt.substitute(value=v)

            except IndexError:
                pass
        cells = map(lambda v: fmt(v), cells_info)
        return JupyterDisplay.row_fmt.substitute(cells=" ".join(cells))
# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
