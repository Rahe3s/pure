import ipaddress
from os_brick import constants as brick_constants
from cinder import interface
from oslo_log import log as logging
from base_driver import PureBaseVolumeDriver
from cinder.common import constants
from.utils import pure_driver_debug_trace
from .exceptions import *
from .constants import *
try:
    from pypureclient import flasharray
except ImportError:
    flasharray = None
from cinder import exception
from cinder.i18n import _
from cinder import utils

from cinder.volume.drivers.san import san
from cinder.volume import volume_utils

LOG = logging.getLogger(__name__)


@interface.volumedriver
class PureISCSIDriver(PureBaseVolumeDriver, san.SanISCSIDriver):
    """OpenStack Volume Driver to support Pure Storage FlashArray.

    This version of the driver enables the use of iSCSI for
    the underlying storage connectivity with the FlashArray.
    """

    VERSION = "20.0.iscsi"

    def __init__(self, *args, **kwargs):
        execute = kwargs.pop("execute", utils.execute)
        super(PureISCSIDriver, self).__init__(execute=execute, *args, **kwargs)
        self._storage_protocol = constants.ISCSI

    def _get_host(self, array, connector, remote=False):
        """Return dict describing existing Purity host object or None."""
        if remote:
            hosts = list(
                getattr(
                    array.get_hosts(
                        filter="iqns='"
                        + connector["initiator"]
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
                        filter="iqns='"
                        + connector["initiator"]
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
        if (self._is_vol_in_pod(pure_vol_name) and
                self._is_active_cluster_enabled and
                not self._failed_over_primary_array):
            target_arrays += self._uniform_active_cluster_target_arrays

        chap_username = None
        chap_password = None
        if self.configuration.use_chap_auth:
            (chap_username, chap_password) = self._get_chap_credentials(
                connector['host'], connector["initiator"])

        targets = []
        for array in target_arrays:
            connection = self._connect(array, pure_vol_name, connector,
                                       chap_username, chap_password)
            if not connection[0].lun:
                # Swallow any exception, just warn and continue
                LOG.warning("self._connect failed.")
                continue
            target_ports = self._get_target_iscsi_ports(array)
            targets.append({
                "connection": connection,
                "ports": target_ports,
            })

        properties = self._build_connection_properties(targets)
        properties["data"]["wwn"] = self._get_wwn(pure_vol_name)

        if self.configuration.use_chap_auth:
            properties["data"]["auth_method"] = "CHAP"
            properties["data"]["auth_username"] = chap_username
            properties["data"]["auth_password"] = chap_password

        return properties

    def _build_connection_properties(self, targets):
        props = {
            "driver_volume_type": "iscsi",
            "data": {
                "target_discovered": False,
                "discard": True,
                "addressing_mode": brick_constants.SCSI_ADDRESSING_SAM2,
            },
        }

        if self.configuration.pure_iscsi_cidr_list:
            iscsi_cidrs = self.configuration.pure_iscsi_cidr_list
            if self.configuration.pure_iscsi_cidr != "0.0.0.0/0":
                LOG.warning("pure_iscsi_cidr was ignored as "
                            "pure_iscsi_cidr_list is set")
        else:
            iscsi_cidrs = [self.configuration.pure_iscsi_cidr]

        check_iscsi_cidrs = [
            ipaddress.ip_network(item) for item in iscsi_cidrs
        ]

        target_luns = []
        target_iqns = []
        target_portals = []

        # Aggregate all targets together if they're in the allowed CIDR. We may
        # end up with different LUNs for different target iqn/portal sets (ie.
        # it could be a unique LUN for each FlashArray)
        for target in range(0, len(targets)):
            port_iter = iter(targets[target]["ports"])
            for port in port_iter:
                # Check to ensure that the portal IP is in the iSCSI target
                # CIDR before adding it
                target_portal = port.portal
                portal, p_port = target_portal.rsplit(':', 1)
                portal = portal.strip('[]')
                check_ip = ipaddress.ip_address(portal)
                for check_cidr in check_iscsi_cidrs:
                    if check_ip in check_cidr:
                        target_luns.append(
                            targets[target]["connection"][0].lun)
                        target_iqns.append(port.iqn)
                        target_portals.append(target_portal)

        LOG.info("iSCSI target portals that match CIDR range: '%s'",
                 target_portals)
        LOG.info("iSCSI target IQNs that match CIDR range: '%s'",
                 target_iqns)

        # If we have multiple ports always report them.
        if target_luns and target_iqns and target_portals:
            props["data"]["target_luns"] = target_luns
            props["data"]["target_iqns"] = target_iqns
            props["data"]["target_portals"] = target_portals

        return props

    def _get_target_iscsi_ports(self, array):
        """Return list of iSCSI-enabled port descriptions."""
        ports = self._get_valid_ports(array)
        iscsi_ports = [port for port in ports if getattr(port, "iqn", None)]
        if not iscsi_ports:
            raise PureDriverException(
                reason=_("No iSCSI-enabled ports on target array."))
        return iscsi_ports

    @staticmethod
    def _generate_chap_secret():
        return volume_utils.generate_password()

    def _get_chap_secret_from_init_data(self, initiator):
        data = self.driver_utils.get_driver_initiator_data(initiator)
        if data:
            for d in data:
                if d["key"] == CHAP_SECRET_KEY:
                    return d["value"]
        return None

    def _get_chap_credentials(self, host, initiator):
        username = host
        password = self._get_chap_secret_from_init_data(initiator)
        if not password:
            password = self._generate_chap_secret()
            success = self.driver_utils.insert_driver_initiator_data(
                initiator, CHAP_SECRET_KEY, password)
            if not success:
                # The only reason the save would have failed is if someone
                # else (read: another thread/instance of the driver) set
                # one before we did. In that case just do another query.
                password = self._get_chap_secret_from_init_data(initiator)

        return username, password

    @utils.retry(PureRetryableException,
                 retries=HOST_CREATE_MAX_RETRIES)
    def _connect(self, array, vol_name, connector,
                 chap_username, chap_password):
        """Connect the host and volume; return dict describing connection."""
        iqn = connector["initiator"]
        hosts = self._get_host(array, connector, remote=False)
        host = hosts[0] if len(hosts) > 0 else None
        if host:
            host_name = host.name
            LOG.info("Re-using existing purity host %(host_name)r",
                     {"host_name": host_name})
            if self.configuration.use_chap_auth:
                if not GENERATED_NAME.match(host_name):
                    LOG.error("Purity host %(host_name)s is not managed "
                              "by Cinder and can't have CHAP credentials "
                              "modified. Remove IQN %(iqn)s from the host "
                              "to resolve this issue.",
                              {"host_name": host_name,
                               "iqn": connector["initiator"]})
                    raise PureDriverException(
                        reason=_("Unable to re-use a host that is not "
                                 "managed by Cinder with use_chap_auth=True,"))
                elif chap_username is None or chap_password is None:
                    LOG.error("Purity host %(host_name)s is managed by "
                              "Cinder but CHAP credentials could not be "
                              "retrieved from the Cinder database.",
                              {"host_name": host_name})
                    raise PureDriverException(
                        reason=_("Unable to re-use host with unknown CHAP "
                                 "credentials configured."))
        else:
            personality = self.configuration.safe_get('pure_host_personality')
            host_name = self._generate_purity_host_name(connector["host"])
            LOG.info("Creating host object %(host_name)r with IQN:"
                     " %(iqn)s.", {"host_name": host_name, "iqn": iqn})
            res = array.post_hosts(names=[host_name],
                                   host=flasharray.HostPost(iqns=[iqn]))
            if res.status_code == 400:
                if (ERR_MSG_ALREADY_EXISTS in res.errors[0].message or
                        ERR_MSG_ALREADY_IN_USE in res.errors[0].message):
                    # If someone created it before we could just retry, we will
                    # pick up the new host.
                    LOG.debug('Unable to create host: %s',
                              res.errors[0].message)
                    raise PureRetryableException()

            if personality:
                self.set_personality(array, host_name, personality)

            if self.configuration.use_chap_auth:
                res = array.patch_hosts(names=[host_name],
                                        host=flasharray.HostPatch(
                                            chap=flasharray.Chap(
                                                host_user=chap_username,
                                                host_password=chap_password)))
                if (res.status_code == 400 and
                        ERR_MSG_HOST_NOT_EXIST in res.errors[0].message):
                    # If the host disappeared out from under us that's ok,
                    # we will just retry and snag a new host.
                    LOG.debug('Unable to set CHAP info: %s',
                              res.errors[0].message)
                    raise PureRetryableException()

        # TODO: Ensure that the host has the correct preferred
        # arrays configured for it.

        connection = self._connect_host_to_vol(array,
                                               host_name,
                                               vol_name)

        return connection
