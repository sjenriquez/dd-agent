# stdlib
import mock

# project
from tests.checks.common import AgentCheckTest


class MockPart(object):
    def __init__(self, device='/dev/sda1', fstype='ext4', mountpoint='/'):
        self.device = device
        self.fstype = fstype
        self.mountpoint = mountpoint


class MockDiskMetrics(object):
    def __init__(self):
        self.total = 5 * 1024
        self.used = 4 * 1024
        self.free = 1 * 1024
        self.percent = 80


class MockInodesMetrics(object):
    def __init__(self):
        self.f_files = 10
        self.f_ffree = 9


class TestCheckDisk(AgentCheckTest):
    CHECK_NAME = 'disk'

    GAUGES_VALUES = {
        'system.disk.total': 5,
        'system.disk.used': 4,
        'system.disk.free': 1,
        'system.disk.in_use': .80,
        'system.fs.inodes.total': 10,
        'system.fs.inodes.used': 1,
        'system.fs.inodes.free': 9,
        'system.fs.inodes.in_use': .10
    }

    def test_device_exclusion_logic(self):
        self.run_check({'instances': [{'use_mount': 'no',
                                       'excluded_filesystems': ['aaaaaa'],
                                       'excluded_disks': ['bbbbbb'],
                                       'excluded_disk_re': '^tev+$'}]},
                       mocks={'collect_metrics': (lambda: None)})
        # should pass, default mock is a normal disk
        self.assertFalse(self.check._exclude_disk(MockPart()))

        # standard fake devices
        self.assertTrue(self.check._exclude_disk(MockPart(device='udev')))

        # excluded filesystems list
        self.assertTrue(self.check._exclude_disk(MockPart(fstype='aaaaaa')))
        self.assertFalse(self.check._exclude_disk(MockPart(fstype='a')))

        # excluded devices list
        self.assertTrue(self.check._exclude_disk(MockPart(device='bbbbbb')))
        self.assertFalse(self.check._exclude_disk(MockPart(device='b')))

        # excluded devices regex
        self.assertTrue(self.check._exclude_disk(MockPart(device='tevvv')))
        self.assertFalse(self.check._exclude_disk(MockPart(device='tevvs')))

    @mock.patch('psutil.disk_partitions', return_value=[MockPart()])
    @mock.patch('psutil.disk_usage', return_value=MockDiskMetrics())
    @mock.patch('os.statvfs', return_value=MockInodesMetrics())
    def test_tag_by_filesystem(self, mock_partitions, mock_usage, mock_inodes):
        self.run_check({'instances': [{'tag_by_filesystem': 'yes'}]})

        # Assert metrics
        tags = ['ext4']
        for metric, value in self.GAUGES_VALUES.iteritems():
            self.assertMetric(metric, value=value, tags=tags)

        self.coverage_report()
