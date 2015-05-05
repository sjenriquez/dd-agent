# stdlib
import re

# project
from checks import AgentCheck
from config import _is_affirmative
from util import Platform
from utils.subprocess_output import get_subprocess_output


class Disk(AgentCheck):
    """ Collects metrics about the machine's disks. """
    DF_COMMAND = 'df'
    FAKE_DEVICES = ['none', 'udev']

    def __init__(self, name, init_config, agentConfig, instances=None):
        if instances is not None and len(instances) > 1:
            raise Exception("Diskcheck only supports one configured instance.")
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

        self.collect_disk_metrics()
        self.collect_inodes_metrics()

    def collect_disk_metrics(self):
        self._collect_command_metrics([self.DF_COMMAND, '-k'])

    def collect_inodes_metrics(self):
        self._collect_command_metrics([self.DF_COMMAND, '-i'], is_inode=True)

    def _collect_command_metrics(self, df_command, is_inode=False):
        # FIXME: find a real crossplatform tool to get all disk info
        # df doesn't have a real standard, so it's not possible to get
        # filesystem info with it on Mac
        if not Platform.is_darwin():
            df_command.append('-T')
        df_out = get_subprocess_output(df_command, self.log)
        self.log.debug(df_out)
        for device in self._list_devices(df_out):
            self.log.debug("Passed: {0}".format(device))
            self._send_metrics(device, is_inode=is_inode)

    def _send_metrics(self, device, is_inode=False):
        if is_inode:
            prefix = 'system.fs.inodes.'
        else:
            prefix = 'system.disk.'
        device, tags = self._extract_tags(device)
        device_name = self._extract_device_name(device)
        for metric_name, value in self._extract_metrics(device,
                                                        is_inode=is_inode):
            self.gauge(prefix + metric_name, value, tags=tags,
                       device_name=device_name)

    def _extract_tags(self, device):
        tags = []
        if not Platform.is_darwin():
            if self._tag_by_filesystem:
                tags = [device.pop(1)]
            else:
                del device[1]
        return device, tags

    def _extract_device_name(self, device):
        if self._use_mount:
            return device[-1]
        else:
            return device[0]

    def _extract_metrics(self, device, is_inode):
        result = []
        # We already removed the filesystem column, so all is good
        if is_inode and Platform.is_darwin() or Platform.is_freebsd():
            result.append(['used', float(device[5])])
            result.append(['free', float(device[6])])
            result.append(['total', float(device[5]) + float(device[6])])
            if len(device[7]) > 1 and device[7][-1] == '%':
                result.append(['in_use',  float(device[7][:-1]) / 100.0])
        else:
            # device is (for debian)
            # ["/dev/sda1", 524288,  171642,  352646, "33%", "/"]
            # and it even works on Mac, which has more columns
            # (already contains inodes), but we just don't care
            # ["/dev/sda1", 524288,  171642,  352646, "33%", 48766325, 12219017, 80%, "/"]
            result.append(['total', float(device[1])])
            result.append(['used', float(device[2])])
            result.append(['free', float(device[3])])
            if len(device[4]) > 1 and device[4][-1] == '%':
                result.append(['in_use',  float(device[4][:-1]) / 100.0])
        return result

    @staticmethod
    def _is_number(a_string):
        try:
            float(a_string)
        except ValueError:
            return False
        return True

    def _is_real_device(self, device):
        """
        Return true if we should track the given device name
        and false otherwise.
        """
        # First, skip empty lines.
        # Then filter out fake devices, (device[0] is the device name)
        # and finally filter our fake hosts like 'map -hosts'.
        # device[1] should be the first number (if filesystem is not displayed,
        # so only for Mac; otherwise it's device[2]). For example:
        #    Filesystem    1024-blocks     Used Available Capacity  Mounted on
        #    /dev/disk0s2    244277768 88767396 155254372    37%    /
        #    map -hosts              0        0         0   100%    /net
        first_block_number = 1 if Platform.is_darwin() else 2
        return (device and len(device) > 1 and
                device[0] not in self.FAKE_DEVICES and
                self._is_number(device[first_block_number]))

    def _keep_device(self, device):
        # device is for Unix
        # [/dev/disk0s2, ext4, 244277768, 88767396, 155254372, 37%, /]
        # except Mac (no filesystem info)
        # [/dev/disk0s2, 244277768, 88767396, 155254372, 37%, /]
        return (self._is_real_device(device) and
                device[0] not in self._excluded_disks and
                not self._excluded_disk_re.match(device[0]) and
                device[1] not in self._excluded_filesystems)

    def _flatten_devices(self, devices):
        # Some volumes are stored on their own line. Rejoin them here.
        previous = None
        for parts in devices:
            if len(parts) == 1:
                previous = parts[0]
            elif previous and self._is_number(parts[0]):
                # collate with previous line
                parts.insert(0, previous)
                previous = None
            else:
                previous = None
        return devices

    def _list_devices(self, df_output):
        """
        Given raw output for the df command, transform it into a normalized
        list devices. A 'device' is a list with fields corresponding to the
        output of df output on each platform.
        """
        all_devices = [l.strip().split() for l in df_output.split("\n")]

        # Skip the header row and empty lines.
        raw_devices = [l for l in all_devices[1:] if l]

        # Flatten the disks that appear in the mulitple lines.
        flattened_devices = self._flatten_devices(raw_devices)

        # Filter fake or unwanteddisks.
        return [d for d in flattened_devices if self._keep_device(d)]
