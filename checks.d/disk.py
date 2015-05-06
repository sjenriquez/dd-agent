# stdlib
import os
import re

# 3p
import psutil

# project
from checks import AgentCheck
from config import _is_affirmative
from util import Platform


class Disk(AgentCheck):
    """ Collects metrics about the machine's disks. """
    METRIC_DISK = 'system.disk.{0}'
    METRIC_INODE = 'system.fs.inodes.{0}'
    FAKE_DEVICES = ['udev', 'sysfs', 'rpc_pipefs', 'proc', 'devpts']

    def __init__(self, name, init_config, agentConfig, instances=None):
        if instances is not None and len(instances) > 1:
            raise Exception("Disk check only supports one configured instance.")
        AgentCheck.__init__(self, name, init_config,
                            agentConfig, instances=instances)

    def check(self, instance):
        """Get disk space/inode stats"""
        # First get the configuration.
        self._use_mount = _is_affirmative(instance.get('use_mount', ''))
        self._excluded_filesystems = instance.get('excluded_filesystems', [])
        self._excluded_disks = instance.get('excluded_disks', [])
        self._excluded_disk_re = re.compile(instance.get('excluded_disk_re', '^$'))
        self._tag_by_filesystem = _is_affirmative(instance.get('tag_by_filesystem', ''))
        self._all_partitions = _is_affirmative(instance.get('all_partitions', 'yes'))

        self.collect_metrics()

    def collect_metrics(self):
        for part in psutil.disk_partitions(all=self._all_partitions):
            # we check all exclude conditions
            if self._exclude_disk(part):
                continue
            tags = [part.fstype] if self._tag_by_filesystem else []
            device_name = part.mountpoint if self._use_mount else part.device
            for metric_name, metric_value in self._collect_part_metrics(part):
                self.gauge(metric_name, metric_value,
                           tags=tags, device_name=device_name)

    def _exclude_disk(self, part):
        # skip cd-rom drives with no disk in it; they may raise
        # ENOENT, pop-up a Windows GUI error for a non-ready
        # partition or just hang;
        # and all the other excluded disks
        return ((Platform.is_win32() and ('cdrom' in part.opts or
                                          part.fstype == '')) or
                part.device in self.FAKE_DEVICES or
                part.device in self._excluded_disks or
                self._excluded_disk_re.match(part.device) or
                part.fstype in self._excluded_filesystems)

    def _collect_part_metrics(self, part):
        usage = psutil.disk_usage(part.mountpoint)
        metrics = {}
        for name in ['total', 'used', 'free']:
            # For legacy reasons,  the standard unit it kB
            metrics[self.METRIC_DISK.format(name)] = getattr(usage, name) / 1024.0
        # FIXME: 6.x, use percent, a lot more logical than in_use
        metrics[self.METRIC_DISK.format('in_use')] = usage.percent / 100.0
        if Platform.is_unix():
            inodes = os.statvfs(part.mountpoint)
            if inodes.f_files != 0:
                total = inodes.f_files
                free = inodes.f_ffree
                metrics[self.METRIC_INODE.format('total')] = total
                metrics[self.METRIC_INODE.format('free')] = free
                metrics[self.METRIC_INODE.format('used')] = total - free
                # FIXME: 6.x, use percent, a lot more logical than in_use
                metrics[self.METRIC_INODE.format('in_use')] = \
                    (total - free) / float(total)

        return metrics.iteritems()
