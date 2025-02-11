import ipaddress
from oslo_log import log as logging
from oslo_utils import excutils
from oslo_utils import units
from packaging import version
from cinder import exception
from cinder.i18n import _
from cinder import utils
from cinder.volume import volume_utils
try:
    from pypureclient import flasharray
except ImportError:
    flasharray = None
from cinder import interface
from cinder.common import constants
from cinder.volume import driver
from.utils import pure_driver_debug_trace
from .base_driver import PureBaseVolumeDriver
from .constants import NVME_PORT,HOST_CREATE_MAX_RETRIES,ERR_MSG_ALREADY_EXISTS,ERR_MSG_ALREADY_IN_USE
from .exceptions import *

LOG = logging.getLogger(__name__)


@interface.volumedriver
class PureNVMEDriver(PureBaseVolumeDriver, driver.BaseVD):
    """OpenStack Volume Driver to support Pure Storage FlashArray.

    This version of the driver enables the use of NVMe over different
    transport types for the underlying storage connectivity with the
    FlashArray.
    """

    VERSION = "20.0.nvme"

    def __init__(self, *args, **kwargs):
        execute = kwargs.pop("execute", utils.execute)
        super(PureNVMEDriver, self).__init__(execute=execute,
                                             *args, **kwargs)
        if self.configuration.pure_nvme_transport == "roce":
            self.transport_type = "rdma"
            self._storage_protocol = constants.NVMEOF_ROCE
        else:
            self.transport_type = "tcp"
            self._storage_protocol = constants.NVMEOF_TCP

    def _get_nguid(self, pure_vol_name):
        """Return the NGUID based on the volume's serial number

        The NGUID is constructed from the volume serial number and
        3 octet OUI

        // octet 0:              padding
        // octets 1 - 7:         first 7 octets of volume serial number
        // octets 8 - 10:        3 octet OUI (24a937)
        // octets 11 - 15:       last 5 octets of volume serial number
        """
        array = self._get_current_array()
        volume_info = list(array.get_volumes(names=[pure_vol_name]).items)[0]
        nguid = ("00" + volume_info.serial[0:14] +
                 "24a937" + volume_info.serial[-10:])
        return nguid.lower()

    def _get_host(self, array, connector, remote=False):
        """Return a list of dicts describing existing host objects or None."""
        if remote:
            hosts = list(
                getattr(
                    array.get_hosts(
                        filter="nqns='"
                        + connector["nqn"]
                        + "' and not is_local"
                    ),
                    "items",
                    []
                )
            )
        else:
            hosts = list(
                getattr(
                    array.get_hosts(
                        filter="nqns='"
                        + connector["nqn"]
                        + "' and is_local"
                    ),
                    "items",
                    []
                )
            )
        return hosts

    @pure_driver_debug_trace
    def initialize_connection(self, volume, connector):
        """Allow connection to connector and return connection info."""
        pure_vol_name = self._get_vol_name(volume)
        target_arrays = [self._get_current_array()]
        if (
            self._is_vol_in_pod(pure_vol_name)
            and self._is_active_cluster_enabled and
            not self._failed_over_primary_array
        ):
            target_arrays += self._uniform_active_cluster_target_arrays

        targets = []
        for array in target_arrays:
            connection = self._connect(array, pure_vol_name, connector)
            array_info = list(self._array.get_arrays().items)[0]
            # Minimum NVMe-TCP support is 6.4.2, but at 6.6.0 Purity
            # changes from using LUN to NSID
            if version.parse(array_info.version) < version.parse(
                '6.6.0'
            ):
                if not connection[0].lun:
                    # Swallow any exception, just warn and continue
                    LOG.warning("self._connect failed.")
                    continue
            else:
                if not connection[0].nsid:
                    # Swallow any exception, just warn and continue
                    LOG.warning("self._connect failed.")
                    continue
            target_ports = self._get_target_nvme_ports(array)
            targets.append(
                {
                    "connection": connection,
                    "ports": target_ports,
                }
            )
        properties = self._build_connection_properties(targets)

        properties["data"]["volume_nguid"] = self._get_nguid(pure_vol_name)

        return properties

    def _build_connection_properties(self, targets):
        props = {
            "driver_volume_type": "nvmeof",
            "data": {
                "discard": True,
            },
        }

        if self.configuration.pure_nvme_cidr_list:
            nvme_cidrs = self.configuration.pure_nvme_cidr_list
            if self.configuration.pure_nvme_cidr != "0.0.0.0/0":
                LOG.warning(
                    "pure_nvme_cidr was ignored as "
                    "pure_nvme_cidr_list is set"
                )
        else:
            nvme_cidrs = [self.configuration.pure_nvme_cidr]

        check_nvme_cidrs = [
            ipaddress.ip_network(item) for item in nvme_cidrs
        ]

        target_luns = []
        target_nqns = []
        target_portals = []

        array_info = list(self._array.get_arrays().items)[0]
        # Aggregate all targets together, we may end up with different
        # namespaces for different target nqn/subsys sets (ie. it could
        # be a unique namespace for each FlashArray)
        for target in range(0, len(targets)):
            for port in targets[target]["ports"]:
                # Check to ensure that the portal IP is in the NVMe target
                # CIDR before adding it
                target_portal = port.portal
                if target_portal and port.nqn:
                    portal, p_port = target_portal.rsplit(':', 1)
                    portal = portal.strip("[]")
                    check_ip = ipaddress.ip_address(portal)
                    for check_cidr in check_nvme_cidrs:
                        if check_ip in check_cidr:
                            # Minimum NVMe-TCP support is 6.4.2,
                            # but at 6.6.0 Purity changes from using LUN to
                            # NSID
                            if version.parse(
                                array_info.version
                            ) < version.parse("6.6.0"):
                                target_luns.append(
                                    targets[target]["connection"][0].lun)
                            else:
                                target_luns.append(
                                    targets[target]["connection"][0].nsid)
                            target_nqns.append(port.nqn)
                            target_portals.append(
                                (portal, NVME_PORT, self.transport_type)
                            )
        LOG.debug(
            "NVMe target portals that match CIDR range: '%s'", target_portals
        )

        # If we have multiple ports always report them.
        if target_luns and target_nqns:
            props["data"]["portals"] = target_portals
            props["data"]["target_nqn"] = target_nqns[0]
        else:
            raise PureDriverException(
                reason=_("No approrpiate nvme ports on target array.")
            )

        return props

    def _get_target_nvme_ports(self, array):
        """Return list of correct nvme-enabled port descriptions."""
        ports = self._get_valid_ports(array)
        valid_nvme_ports = []
        nvme_ports = [port for port in ports if getattr(port, "nqn", None)]
        for port in range(0, len(nvme_ports)):
            if "ETH" in nvme_ports[port].name:
                port_detail = list(array.get_network_interfaces(
                    names=[nvme_ports[port].name]
                ).items)[0]
                if port_detail.services[0] == "nvme-" + \
                        self.configuration.pure_nvme_transport:
                    valid_nvme_ports.append(nvme_ports[port])
        if not nvme_ports:
            raise PureDriverException(
                reason=_("No %(type)s enabled ports on target array.") %
                {"type": self._storage_protocol}
            )
        return valid_nvme_ports

    @utils.retry(PureRetryableException, retries=HOST_CREATE_MAX_RETRIES)
    def _connect(self, array, vol_name, connector):
        """Connect the host and volume; return dict describing connection."""
        nqn = connector["nqn"]
        hosts = self._get_host(array, connector, remote=False)
        host = hosts[0] if len(hosts) > 0 else None
        if host:
            host_name = host.name
            LOG.info(
                "Re-using existing purity host %(host_name)r",
                {"host_name": host_name},
            )
        else:
            personality = self.configuration.safe_get('pure_host_personality')
            host_name = self._generate_purity_host_name(connector["host"])
            LOG.info(
                "Creating host object %(host_name)r with NQN:" " %(nqn)s.",
                {"host_name": host_name, "nqn": connector["nqn"]},
            )
            res = array.post_hosts(names=[host_name],
                                   host=flasharray.HostPost(nqns=[nqn]))
            if res.status_code == 400 and (
                    ERR_MSG_ALREADY_EXISTS in res.errors[0].message
                    or ERR_MSG_ALREADY_IN_USE in res.errors[0].message):
                # If someone created it before we could just retry, we will
                # pick up the new host.
                LOG.debug("Unable to create host: %s",
                          res.errors[0].message)
                raise PureRetryableException()

            if personality:
                self.set_personality(array, host_name, personality)

        # TODO: Ensure that the host has the correct preferred
        # arrays configured for it.

        return self._connect_host_to_vol(array, host_name, vol_name)
