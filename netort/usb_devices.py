import usb1
import logging
import weakref

import libusb1

logger = logging.getLogger(__name__)


class UsbHandler(object):
    """ Usb handler"""

    ANDROID_USB_SETTINGS = (0xFF, 0x42, 0x01)  # (class, subclass, proto), adb.h
    APPLE_VENDOR_ID = 0x05ac
    IPHONE_PRODUCT_ID = 0x12a8
    IPAD_PRODUCT_ID = 0x12ab

    def __init__(self, source=None, device_type='android', timeout=10000):
        super(UsbHandler, self).__init__()
        self.timeout = timeout
        self.source = source
        self.source_type = 'usb' if self.source.startswith('usb:') else 'serial'
        self.device_type = device_type
        self.interface_number = None
        self.__max_read_packet_len = 0
        self.device = None
        self.settings = None
        self.handle = None

    def initialize(self):
        self.device, self.settings = self.get_device()
        self.__get_endpoints()
        self.open()

    @staticmethod
    def _get_devices_by_usb_settings(related_usb_settings):
        ctx = usb1.USBContext()
        return [
            (device, settings)
            for device in ctx.getDeviceList(skip_on_error=True) for settings in device.iterSettings()
            if (settings.getClass(), settings.getSubClass(), settings.getProtocol()) == related_usb_settings
        ]

    @staticmethod
    def _get_devices_by_product_and_vendor_id(vendor_id, product_id):
        ctx = usb1.USBContext()
        return [
            (device, None)
            for device in ctx.getDeviceList(skip_on_error=True)
            if device.getVendorID() == vendor_id and device.getProductID() == product_id
        ]

    def get_devices(self):
        return {
            'ipdas': self._get_devices_by_product_and_vendor_id(self.APPLE_VENDOR_ID, self.IPAD_PRODUCT_ID),
            'iphones': self._get_devices_by_product_and_vendor_id(self.APPLE_VENDOR_ID, self.IPHONE_PRODUCT_ID),
            'androids': self._get_devices_by_usb_settings(self.ANDROID_USB_SETTINGS)
        }

    def get_device(self):
        devices_list = self._get_devices_by_usb_settings(self.ANDROID_USB_SETTINGS)
        logger.debug('Related devices list: %s', devices_list)
        matched_devices = None
        if devices_list:
            if self.source_type == 'usb':
                matched_devices = [
                    (device, settings) for device, settings in devices_list
                    if [device.getBusNumber()] + device.getPortNumberList() == self.source
                ]
            elif self.source_type == 'serial':
                matched_devices = [
                    (device, settings) for device, settings in devices_list
                    if device.getSerialNumber() == self.source
                ]
            else:
                matched_devices = []
        else:
            raise ValueError('No devices found. Is it plugged in and in appropriate state?')
        logger.debug('Matched devices list: %s', matched_devices)
        if not matched_devices:
            raise ValueError('No suitable devices found for source: %s', self.source)
        if len(matched_devices) > 1:
            raise ValueError(
                'There are more than 1 devices found for this source: %s. Devices: %s',
                self.source, matched_devices
            )
        return matched_devices[0]

    def __get_endpoints(self):
        for endpoint in self.settings.iterEndpoints():
            address = endpoint.getAddress()
            if address & libusb1.USB_ENDPOINT_DIR_MASK:
                self.__read_endpoint = address
                self.__max_read_packet_len = endpoint.getMaxPacketSize()
            else:
                self.__write_endpoint = address
        if not self.__read_endpoint or not self.__write_endpoint:
            raise RuntimeError('USB endpoints not found')

    def open(self):
        handle = self.device.open()
        interface = self.settings.getNumber()
        if handle.kernelDriverActive(interface):
            handle.detachKernelDriver(interface)
        handle.claimInterface(interface)
        self.handle = handle
        self.interface_number = interface
        logger.debug('Opened usb handler: %s. Iface: %s', self.handle, self.interface_number)
        weakref.ref(self, self.close)  # When this object is deleted, make sure it's closed.

    def close(self):
        logger.info('Closing handle...')
        try:
            self.handle.releaseInterface(self.interface_number)
            self.handle.close()
        except usb1.USBError:
            logger.warning('USBError while closing handle %s:', exc_info=True)
        finally:
            self.handle = None

    def flush(self):
        while True:
            try:
                self.read(self.__max_read_packet_len)
            except ReadFailedError as e:
                if e.usb_error.value == libusb1.LIBUSB_ERROR_TIMEOUT:
                    break
                raise

    def write(self, data):
        if self.handle is None:
            raise WriteFailedError(
                'This handle has been closed, probably due to another being opened.',
                None)
        try:
            self.handle.bulkWrite(self.__write_endpoint, data, timeout=self.timeout)
        except usb1.USBError:
            logger.warning('Usb write failed, data: %s', data, exc_info=True)
            raise

    def read(self, length):
        if self.handle is None:
            raise ReadFailedError(
                'This handle has been closed, probably due to another being opened.',
                None)
        try:
            chunk = self.handle.bulkRead(self.__read_endpoint, length, timeout=self.timeout)
        except usb1.USBError:
            logger.warning('Usb read failed', exc_info=True)
            raise
        else:
            return bytearray(chunk)


class CommonUsbError(Exception):
    """Base class for handlers communication errors."""


class LibusbWrappingError(CommonUsbError):
    """Wraps libusb1 errors while keeping its original usefulness.

    Attributes:
      usb_error: Instance of libusb1.USBError
    """

    def __init__(self, msg, usb_error):
        super(LibusbWrappingError, self).__init__(msg)
        self.usb_error = usb_error

    def __str__(self):
        return '%s: %s' % (
            super(LibusbWrappingError, self).__str__(), str(self.usb_error))


class WriteFailedError(LibusbWrappingError):
    """Raised when the device doesn't accept our command."""


class ReadFailedError(LibusbWrappingError):
    """Raised when the device doesn't respond to our commands."""
