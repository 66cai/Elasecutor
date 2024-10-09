# -*- coding: utf-8 -*-
import argparse
import os
import sched
import sys
import time
import psutil
from collections import defaultdict


# 定义资源监控器类
class ResMonitor:
    def __init__(self, outfile_name=None, flush=False):
        print('Resource monitor started.', file=sys.stderr)
        self.ncores = psutil.cpu_count()  # 获取 CPU 核心数
        self.outfile = open(outfile_name, 'w') if outfile_name else sys.stdout
        self.flush = flush
        self.prev_disk_stat = psutil.disk_io_counters()
        self.starttime = int(time.time())
        self._write_header()  # 写入文件标题

    def _write_header(self):
        header = (
            'Timestamp, Uptime, NCPU, %CPU, '
            + ', '.join([f'%CPU{i}' for i in range(self.ncores)]) +
            ', %MEM, mem.total.MB, mem.used.MB, mem.avail.MB, mem.free.MB' +
            ', %SWAP, swap.total.MB, swap.used.MB, swap.free.MB' +
            ', io.read, io.write, io.read.MB, io.write.MB, io.read.ms, io.write.ms\n'
        )
        self.outfile.write(header)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        if self.outfile is not sys.stdout:
            self.outfile.close()
        print('Resource monitor closed.', file=sys.stderr)

    def poll_stat(self):
        timestamp = int(time.time())
        uptime = timestamp - self.starttime
        total_cpu_percent = psutil.cpu_percent(percpu=False)
        percpu_percent = psutil.cpu_percent(percpu=True)
        mem_stat = psutil.virtual_memory()
        swap_stat = psutil.swap_memory()
        disk_stat = psutil.disk_io_counters()

        line = (
            f"{timestamp}, {uptime}, {self.ncores}, {total_cpu_percent * self.ncores}, "
            + ', '.join(map(str, percpu_percent)) +
            f", {mem_stat.percent}, {mem_stat.total >> 20}, {mem_stat.used >> 20}, "
            f"{mem_stat.available >> 20}, {mem_stat.free >> 20}, "
            f"{swap_stat.percent}, {swap_stat.total >> 20}, {swap_stat.used >> 20}, "
            f"{swap_stat.free >> 20}, {disk_stat.read_count - self.prev_disk_stat.read_count}, "
            f"{disk_stat.write_count - self.prev_disk_stat.write_count}, "
            f"{(disk_stat.read_bytes - self.prev_disk_stat.read_bytes) >> 20}, "
            f"{(disk_stat.write_bytes - self.prev_disk_stat.write_bytes) >> 20}, "
            f"{disk_stat.read_time - self.prev_disk_stat.read_time}, "
            f"{disk_stat.write_time - self.prev_disk_stat.write_time}"
        )

        self.outfile.write(line + '\n')
        if self.flush:
            self.outfile.flush()
        self.prev_disk_stat = disk_stat


# 定义网络接口监控器类
class NetworkInterfaceMonitor:
    def __init__(self, outfile_pattern='netstat.{nic}.csv', nics=None, flush=False):
        print('NIC monitor started.', file=sys.stderr)
        self.nic_files = {nic_name: self.create_new_logfile(outfile_pattern, nic_name)
                          for nic_name in (nics or []) if nic_name in psutil.net_if_stats()}
        if not self.nic_files:
            raise ValueError('No NIC to monitor.')
        self.prev_stat = {nic: psutil.net_io_counters(pernic=True)[nic] for nic in self.nic_files}
        self.starttime = int(time.time())
        self.flush = flush
        self.poll_stat()  # 初次轮询状态

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        for f in self.nic_files.values():
            f.close()
        print('NIC monitor closed.', file=sys.stderr)

    def create_new_logfile(self, pattern, nic_name):
        f = open(pattern.format(nic=nic_name), 'w')
        f.write('Timestamp, Uptime, NIC, sent.MB, recv.MB, sent.pkts, recv.pkts, err.in, err.out, drop.in, drop.out\n')
        return f

    def poll_stat(self):
        timestamp = int(time.time())
        uptime = timestamp - self.starttime
        net_stat = psutil.net_io_counters(pernic=True)

        for nic, f in self.nic_files.items():
            curr_stat = net_stat[nic]
            prevstat = self.prev_stat[nic]
            f.write(f"{timestamp}, {uptime}, {nic}, "
                    f"{(curr_stat.bytes_sent - prevstat.bytes_sent) >> 20}, "
                    f"{(curr_stat.bytes_recv - prevstat.bytes_recv) >> 20}, "
                    f"{curr_stat.packets_sent - prevstat.packets_sent}, "
                    f"{curr_stat.packets_recv - prevstat.packets_recv}, "
                    f"{curr_stat.errin - prevstat.errin}, "
                    f"{curr_stat.errout - prevstat.errout}, "
                    f"{curr_stat.dropin - prevstat.dropin}, "
                    f"{curr_stat.dropout - prevstat.dropout}\n")
            if self.flush:
                f.flush()
        self.prev_stat = net_stat


# 定义进程集合监控器类
class ProcessSetMonitor:
    BASE_STAT = {
        'io.read': 0,
        'io.write': 0,
        'io.read.MB': 0,
        'io.write.MB': 0,
        'mem.rss.MB': 0,
        '%MEM': 0,
        '%CPU': 0,
    }

    def __init__(self, keywords, pids, outfile_name, flush=False):
        print('ProcessSet monitor started.', file=sys.stderr)
        self.outfile = open(outfile_name, 'w') if outfile_name else sys.stdout
        self.pids = set(pids)
        self.keywords = keywords
        self.flush = flush
        self.outfile.write('Timestamp, Uptime, ' + ', '.join(sorted(self.BASE_STAT.keys())) + '\n')
        self.starttime = int(time.time())
        self.poll_stat()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        if self.outfile is not sys.stdout:
            self.outfile.close()
        print('ProcessSet monitor closed.', file=sys.stderr)

    def _stat_proc(self, proc, visited):
        if proc.pid in visited:
            return
        visited.add(proc.pid)

        try:
            io = proc.io_counters()
            mem_rss = proc.memory_info().rss
            mem_percent = proc.memory_percent('rss')
            cpu_percent = proc.cpu_percent()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return

        return {
            'io.read': io.read_count,
            'io.write': io.write_count,
            'io.read.MB': io.read_bytes >> 20,  # 转换为MB
            'io.write.MB': io.write_bytes >> 20,  # 转换为MB
            'mem.rss.MB': mem_rss >> 20,  # 转换为MB
            '%MEM': mem_percent,
            '%CPU': cpu_percent,
        }

    def poll_stat(self):
        visited = set()
        curr_stat = defaultdict(int)
        timestamp = int(time.time())
        uptime = timestamp - self.starttime

        for proc in psutil.process_iter(attrs=['pid', 'name']):
            try:
                pinfo = proc.info
                if pinfo['pid'] in self.pids or any(k in pinfo['name'].lower() for k in self.keywords):
                    proc_stat = self._stat_proc(proc, visited)
                    if proc_stat:
                        for key, value in proc_stat.items():
                            curr_stat[key] += value
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        line = f"{timestamp}, {uptime}, " + ', '.join(str(curr_stat[k]) for k in sorted(self.BASE_STAT.keys()))
        self.outfile.write(line + '\n')
        if self.flush:
            self.outfile.flush()


# 更改进程优先级
def chprio(prio):
    try:
        psutil.Process(os.getpid()).nice(prio)
    except Exception:
        print('Warning: failed to elevate priority!', file=sys.stderr)


# 信号处理函数
def sigterm(signum, frame):
    raise KeyboardInterrupt()


# 主函数，处理命令行参数并执行监控任务
def main():
    parser = argparse.ArgumentParser(description="Resource Monitor")
    parser.add_argument('--ps-pids', type=int, nargs='*', help='Include the specified PIDs and their children.')
    parser.add_argument('--ps-outfile', type=str, default='resprofile.csv')
    parser.add_argument('--nic', type=str, nargs='*', help='Network interface names to monitor.')
    parser.add_argument('--outfile', type=str, help='Output file for resource monitor.')
    parser.add_argument('--flush', action='store_true', help='Flush output after each line.')
    parser.add_argument('--delay', type=int, default=5, help='Delay in seconds between each poll.')
    args = parser.parse_args()

    args.ps_pids = set(args.ps_pids) if args.ps_pids else set()

    try:
        chprio(-20)  # 设置进程优先级
        scheduler = sched.scheduler(time.time, time.sleep)

        rm = ResMonitor(args.outfile, args.flush)
        enable_nic_mon = args.nic is not None
        nm = NetworkInterfaceMonitor(nics=args.nic, flush=args.flush) if enable_nic_mon else None
        enable_ps_mon = len(args.ps_pids) > 0
        pm = ProcessSetMonitor([], args.ps_pids, args.ps_outfile, args.flush) if enable_ps_mon else None

        i = 1
        starttime = time.time()
        while True:
            scheduler.enterabs(starttime + i * args.delay, priority=2, action=ResMonitor.poll_stat, argument=(rm,))
            if enable_nic_mon:
                scheduler.enterabs(starttime + i * args.delay, priority=1, action=NetworkInterfaceMonitor.poll_stat, argument=(nm,))
            if enable_ps_mon:
                scheduler.enterabs(starttime + i * args.delay, priority=0, action=ProcessSetMonitor.poll_stat, argument=(pm,))
            scheduler.run()
            i += 1
    except KeyboardInterrupt:
        print("Monitoring interrupted. Exiting.")
    finally:
        rm.close()
        if enable_nic_mon:
            nm.close()
        if enable_ps_mon:
            pm.close()


if __name__ == '__main__':
    main()  # 执行主程序
