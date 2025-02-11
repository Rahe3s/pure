import functools
import uuid

from oslo_log import log as logging
from cinder.volume import driver

from cinder.i18n import _
from cinder import interface
from cinder.zonemanager import utils as fczm_utils

from .base_driver import PureBaseVolumeDriver

try:
    from pypureclient import flasharray
except ImportError:
    flasharray = None
from os_brick import constants as brick_constants
from oslo_log import log as logging
from packaging import version
try:
    from pypureclient import flasharray
except ImportError:
    flasharray = None

from cinder.common import constants
from cinder.i18n import _
from cinder import interface

from cinder import utils
from cinder.volume import driver

@interface.volumedriver
class PureFCDriver(PureBaseVolumeDriver, driver.FibreChannelDriver):
    """OpenStack Volume Driver to support Pure Storage FlashArray.

    This version of the driver enables the use of Fibre Channel for
    the underlying storage connectivity with the FlashArray. It fully
    supports the Cinder Fibre Channel Zone Manager.
    """

    VERSION = "20.0.fc"

    def __init__(self, *args, **kwargs):
        execute = kwargs.pop("execute", utils.execute)
        super(PureFCDriver, self).__init__(execute=execute, *args, **kwargs)
        self._storage_protocol = constants.FC
        self._lookup_service = fczm_utils.create_lookup_service()

    def _get_host(self, array, connector, remote=False):
        """Return dict describing existing Purity host object or None."""
        if remote:
            for wwn in connector["wwpns"]:
                hosts = list(
                    getattr(
                        array.get_hosts(
                            filter="wwns='"
                            + wwn.upper()
                            + "' and not is_local"
                        ),
                        "items",
                        []
                    )
                )
        else:
            for wwn in connector["wwpns"]:
                hosts = list(
                    getattr(
                        array.get_hosts(
                            filter="wwns='"
                            + wwn.upper()
                            + "' and is_local"
                        ),
                        "items",
                        []
                    )
                )
        return hosts

    def _get_array_wwns(self, array):
        """Return list of wwns from the array

        Ensure that only true scsi FC ports are selected
        and not any that are enabled for NVMe-based FC with
        an associated NQN.
        """
        ports = self._get_valid_ports(array)
        valid_ports = [port.wwn.replace(":", "") for port in ports if getattr(
            port, "wwn", None) and not getattr(port, "nqn", None)]
        return valid_ports

    @pure_driver_debug_trace
    def initialize_connection(self, volume, connector):
        """Allow connection to connector and return connection info."""
        pure_vol_name = self._get_vol_name(volume)
        target_arrays = [self._get_current_array()]
        if (self._is_vol_in_pod(pure_vol_name) and
                self._is_active_cluster_enabled and
                not self._failed_over_primary_array):
            target_arrays += self._uniform_active_cluster_target_arrays

        target_luns = []
        target_wwns = []
        for array in target_arrays:
            connection = self._connect(array, pure_vol_name, connector)
            if not connection[0].lun:
                # Swallow any exception, just warn and continue
                LOG.warning("self._connect failed.")
                continue
            array_wwns = self._get_array_wwns(array)
            for wwn in array_wwns:
                target_wwns.append(wwn)
                target_luns.append(connection[0].lun)

        # Build the zoning map based on *all* wwns, this could be multiple
        # arrays connecting to the same host with a stretched volume.
        init_targ_map = self._build_initiator_target_map(target_wwns,
                                                         connector)

        properties = {
            "driver_volume_type": "fibre_channel",
            "data": {
                "target_discovered": True,
                "target_lun": target_luns[0],  # For backwards compatibility
                "target_luns": target_luns,
                "target_wwn": target_wwns,
                "target_wwns": target_wwns,
                "initiator_target_map": init_targ_map,
                "discard": True,
                "addressing_mode": brick_constants.SCSI_ADDRESSING_SAM2,
            }
        }
        properties["data"]["wwn"] = self._get_wwn(pure_vol_name)

        fczm_utils.add_fc_zone(properties)
        return properties

    @utils.retry(PureRetryableException,
                 retries=HOST_CREATE_MAX_RETRIES)
    def _connect(self, array, vol_name, connector):
        """Connect the host and volume; return dict describing connection."""
        wwns = connector["wwpns"]
        hosts = self._get_host(array, connector, remote=False)
        host = hosts[0] if len(hosts) > 0 else None

        if host:
            host_name = host.name
            LOG.info("Re-using existing purity host %(host_name)r",
                     {"host_name": host_name})
        else:
            personality = self.configuration.safe_get('pure_host_personality')
            host_name = self._generate_purity_host_name(connector["host"])
            LOG.info("Creating host object %(host_name)r with WWN:"
                     " %(wwn)s.", {"host_name": host_name, "wwn": wwns})
            res = array.post_hosts(names=[host_name],
                                   host=flasharray.HostPost(wwns=wwns))
            if (res.status_code == 400 and
                    (ERR_MSG_ALREADY_EXISTS in res.errors[0].message or
                        ERR_MSG_ALREADY_IN_USE in res.errors[0].message)):
                # If someone created it before we could just retry, we will
                # pick up the new host.
                LOG.debug('Unable to create host: %s',
                          res.errors[0].message)
                raise PureRetryableException()

            if personality:
                self.set_personality(array, host_name, personality)

        # TODO: Ensure that the host has the correct preferred
        # arrays configured for it.

        return self._connect_host_to_vol(array, host_name, vol_name)

    def _build_initiator_target_map(self, target_wwns, connector):
        """Build the target_wwns and the initiator target map."""
        init_targ_map = {}

        if self._lookup_service:
            # use FC san lookup to determine which NSPs to use
            # for the new VLUN.
            dev_map = self._lookup_service.get_device_mapping_from_network(
                connector['wwpns'],
                target_wwns)

            for fabric_name in dev_map:
                fabric = dev_map[fabric_name]
                for initiator in fabric['initiator_port_wwn_list']:
                    if initiator not in init_targ_map:
                        init_targ_map[initiator] = []
                    init_targ_map[initiator] += fabric['target_port_wwn_list']
                    init_targ_map[initiator] = list(set(
                        init_targ_map[initiator]))
        else:
            init_targ_map = dict.fromkeys(connector["wwpns"], target_wwns)

        return init_targ_map

    @pure_driver_debug_trace
    def terminate_connection(self, volume, connector, **kwargs):
        """Terminate connection."""
        vol_name = self._get_vol_name(volume)
        # None `connector` indicates force detach, then delete all even
        # if the volume is multi-attached.
        multiattach = (connector is not None and
                       self._is_multiattach_to_host(volume.volume_attachment,
                                                    connector["host"]))
        unused_wwns = []

        if self._is_vol_in_pod(vol_name):
            # Try to disconnect from each host, they may not be online though
            # so if they fail don't cause a problem.
            for array in self._uniform_active_cluster_target_arrays:
                no_more_connections = self._disconnect(
                    array, volume, connector, remove_remote_hosts=True,
                    is_multiattach=multiattach)
                if no_more_connections:
                    unused_wwns += self._get_array_wwns(array)

        # Now disconnect from the current array, removing any left over
        # remote hosts that we maybe couldn't reach.
        current_array = self._get_current_array()
        no_more_connections = self._disconnect(current_array,
                                               volume, connector,
                                               remove_remote_hosts=False,
                                               is_multiattach=multiattach)
        if no_more_connections:
            unused_wwns += self._get_array_wwns(current_array)

        properties = {"driver_volume_type": "fibre_channel", "data": {}}
        if len(unused_wwns) > 0:
            init_targ_map = self._build_initiator_target_map(unused_wwns,
                                                             connector)
            properties["data"] = {"target_wwn": unused_wwns,
                                  "initiator_target_map": init_targ_map}

        fczm_utils.remove_fc_zone(properties)
        return properties

