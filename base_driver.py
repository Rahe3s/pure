import distro
import uuid
import math
from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import excutils
from packaging import version
from cinder.i18n import _

try:
    from pypureclient import flasharray
except ImportError:
    flasharray = None

from cinder import context
from cinder import utils
from cinder.common import constants
from cinder.volume.drivers.san import san
from cinder.volume import volume_utils
from cinder.volume import volume_types
from cinder.objects import fields
from .exceptions import PureDriverException,PureRetryableException

from config import PURE_OPTS
from constants import USER_AGENT_BASE
from.utils import pure_driver_debug_trace
from oslo_utils import units
from .constants import *
from cinder import exception
from .config import CONF


LOG = logging.getLogger(__name__)

class PureBaseVolumeDriver(san.SanDriver):
    """Performs volume management on Pure Storage FlashArray."""

    SUPPORTS_ACTIVE_ACTIVE = True
    PURE_QOS_KEYS = ['maxIOPS', 'maxBWS']
    # ThirdPartySystems wiki page
    CI_WIKI_NAME = "Pure_Storage_CI"

    def __init__(self, *args, **kwargs):
        execute = kwargs.pop("execute", utils.execute)
        super(PureBaseVolumeDriver, self).__init__(execute=execute, *args, **kwargs)
        self.configuration.append_config_values(PURE_OPTS)
        self._array = None
        self._storage_protocol = None
        self._backend_name = (self.configuration.volume_backend_name or self.__class__.__name__)
        self._replication_target_arrays = []
        self._active_cluster_target_arrays = []
        self._uniform_active_cluster_target_arrays = []
        self._trisync_pg_name = None
        self._replication_pg_name = None
        self._trisync_name = None
        self._replication_pod_name = None
        self._replication_interval = None
        self._replication_retention_short_term = None
        self._replication_retention_long_term = None
        self._replication_retention_long_term_per_day = None
        self._async_replication_retention_policy = {}
        self._is_replication_enabled = False
        self._is_active_cluster_enabled = False
        self._is_trisync_enabled = False
        self._active_backend_id = kwargs.get('active_backend_id', None)
        self._failed_over_primary_array = None
        self._user_agent = '%(base)s %(class)s/%(version)s (%(platform)s)' % {
            'base': USER_AGENT_BASE,
            'class': self.__class__.__name__,
            'version': self.VERSION,
            'platform': distro.name(pretty=True)
        }

    @classmethod
    def get_driver_options(cls):
        additional_opts = cls._get_oslo_driver_opts(
            'san_ip', 'driver_ssl_cert_verify', 'driver_ssl_cert_path',
            'use_chap_auth', 'replication_device', 'reserved_percentage',
            'max_over_subscription_ratio', 'pure_nvme_transport',
            'pure_nvme_cidr_list', 'pure_nvme_cidr',
            'pure_trisync_enabled', 'pure_trisync_pg_name')
        return PURE_OPTS + additional_opts


    def do_setup(self, context):
        """Performs driver initialization steps that could raise exceptions."""
        if flasharray is None:
            msg = _("Missing 'py-pure-client' python module, ensure the"
                    " library is installed and available.")
            raise PureDriverException(msg)

        # Raises PureDriverException if unable to connect and PureError
        # if unable to authenticate.
        self._array = self._get_flasharray(
            san_ip=self.configuration.san_ip,
            api_token=self.configuration.pure_api_token,
            verify_ssl=self.configuration.driver_ssl_cert_verify,
            ssl_cert_path=self.configuration.driver_ssl_cert_path
        )
        if self._array:
            array_info = list(self._array.get_arrays().items)[0]
            if version.parse(array_info.version) < version.parse(
                '6.1.0'
            ):
                msg = _("FlashArray Purity version less than 6.1.0 "
                        "unsupported. Please upgrade your backend to "
                        "a supported version.")
                raise PureDriverException(msg)
            if version.parse(array_info.version) < version.parse(
                '6.4.2'
            ) and self._storage_protocol == constants.NVMEOF_TCP:
                msg = _("FlashArray Purity version less than 6.4.2 "
                        "unsupported for NVMe-TCP. Please upgrade your "
                        "backend to a supported version.")
                raise PureDriverException(msg)

            self._array.array_name = array_info.name
            self._array.array_id = array_info.id
            self._array.replication_type = None
            self._array.backend_id = self._backend_name
            self._array.preferred = True
            self._array.uniform = True
            self._array.version = array_info.version
            if version.parse(array_info.version) < version.parse(
                '6.3.4'
            ):
                self._array.safemode = False
            else:
                self._array.safemode = True

            LOG.info("Primary array: backend_id='%s', name='%s', id='%s'",
                     self.configuration.config_group,
                     self._array.array_name,
                     self._array.array_id)
        else:
            LOG.warning("self.do_setup failed to set up primary array: %(ip)s",
                        {"ip": self.configuration.san_ip})

        self.do_setup_replication()

        if self.configuration.pure_trisync_enabled:
            # If trisync is enabled check that we have only 1 sync and 1 async
            # replication device set up and that the async target is not the
            # same as any of the sync targets.
            self.do_setup_trisync()

        # If we have failed over at some point we need to adjust our current
        # array based on the one that we have failed over to
        if (self._active_backend_id and
                self._active_backend_id != self._array.backend_id):
            secondary_array = self._get_secondary(self._active_backend_id)
            self._swap_replication_state(self._array, secondary_array)

    def check_for_setup_error(self):
        # Avoid inheriting check_for_setup_error from SanDriver, which checks
        # for san_password or san_private_key, not relevant to our driver.
        pass

    def update_provider_info(self, volumes, snapshots):
        """Ensure we have a provider_id set on volumes.

        If there is a provider_id already set then skip, if it is missing then
        we will update it based on the volume object. We can always compute
        the id if we have the full volume object, but not all driver API's
        give us that info.

        We don't care about snapshots, they just use the volume's provider_id.
        """
        vol_updates = []
        for vol in volumes:
            if not vol.provider_id:
                vol.provider_id = self._get_vol_name(vol)
                vol_name = self._generate_purity_vol_name(vol)
                if vol.metadata:
                    vol_updates.append({
                        'id': vol.id,
                        'provider_id': vol_name,
                        'metadata': {**vol.metadata,
                                     'array_volume_name': vol_name,
                                     'array_name': self._array.array_name},
                    })
                else:
                    vol_updates.append({
                        'id': vol.id,
                        'provider_id': vol_name,
                        'metadata': {'array_volume_name': vol_name,
                                     'array_name': self._array.array_name},
                    })
        return vol_updates, None

    @pure_driver_debug_trace
    def revert_to_snapshot(self, context, volume, snapshot):
        """Is called to perform revert volume from snapshot.

        :param context: Our working context.
        :param volume: the volume to be reverted.
        :param snapshot: the snapshot data revert to volume.
        :return None
        """
        vol_name = self._generate_purity_vol_name(volume)
        if snapshot['cgsnapshot']:
            snap_name = self._get_pgroup_snap_name_from_snapshot(snapshot)
        else:
            snap_name = self._get_snap_name(snapshot)

        LOG.debug("Reverting from snapshot %(snap)s to volume "
                  "%(vol)s", {'vol': vol_name, 'snap': snap_name})

        current_array = self._get_current_array()

        current_array.post_volumes(names=[snap_name], overwrite=True,
                                   volume=flasharray.VolumePost(
                                       source=flasharray.Reference(
                                           name=vol_name)))

    @pure_driver_debug_trace
    def create_volume(self, volume):
        """Creates a volume."""
        qos = None
        vol_name = self._generate_purity_vol_name(volume)
        vol_size = volume["size"] * units.Gi
        ctxt = context.get_admin_context()
        type_id = volume.get('volume_type_id')
        current_array = self._get_current_array()
        if type_id is not None:
            volume_type = volume_types.get_volume_type(ctxt, type_id)
            qos = self._get_qos_settings(volume_type)
        if qos is not None:
            self.create_with_qos(current_array, vol_name, vol_size, qos)
        else:
            if self._array.safemode:
                current_array.post_volumes(names=[vol_name],
                                           with_default_protection=False,
                                           volume=flasharray.VolumePost(
                                               provisioned=vol_size))
            else:
                current_array.post_volumes(names=[vol_name],
                                           volume=flasharray.VolumePost(
                                               provisioned=vol_size))

        return self._setup_volume(current_array, volume, vol_name)

    @pure_driver_debug_trace
    def create_volume_from_snapshot(self, volume, snapshot, cgsnapshot=False):
        """Creates a volume from a snapshot."""
        qos = None
        vol_name = self._generate_purity_vol_name(volume)
        if cgsnapshot:
            snap_name = self._get_pgroup_snap_name_from_snapshot(snapshot)
        else:
            snap_name = self._get_snap_name(snapshot)

        current_array = self._get_current_array()
        ctxt = context.get_admin_context()
        type_id = volume.get('volume_type_id')
        if type_id is not None:
            volume_type = volume_types.get_volume_type(ctxt, type_id)
            qos = self._get_qos_settings(volume_type)

        if self._array.safemode:
            current_array.post_volumes(names=[vol_name],
                                       with_default_protection=False,
                                       volume=flasharray.VolumePost(
                                           source=flasharray.Reference(
                                               name=snap_name)))
        else:
            current_array.post_volume(names=[vol_name],
                                      volume=flasharray.VolumePost(
                                          source=flasharray.Reference(
                                              name=snap_name)))
        self._extend_if_needed(current_array,
                               vol_name,
                               snapshot["volume_size"],
                               volume["size"])
        if qos is not None:
            self.set_qos(current_array, vol_name, qos)
        else:
            current_array.patch_volumes(names=[vol_name],
                                        volume=flasharray.VolumePatch(
                                            qos=flasharray.Qos(
                                                iops_limit=100000000,
                                                bandwidth_limit=549755813888)))

        return self._setup_volume(current_array, volume, vol_name)


    @pure_driver_debug_trace
    def create_cloned_volume(self, volume, src_vref):
        """Creates a clone of the specified volume."""
        vol_name = self._generate_purity_vol_name(volume)
        src_name = self._get_vol_name(src_vref)

        # Check which backend the source volume is on. In case of failover
        # the source volume may be on the secondary array.
        current_array = self._get_current_array()
        current_array.post_volumes(volume=flasharray.VolumePost(
            source=flasharray.Reference(name=src_name)), names=[vol_name])
        self._extend_if_needed(current_array,
                               vol_name,
                               src_vref["size"],
                               volume["size"])

        return self._setup_volume(current_array, volume, vol_name)

    def _extend_if_needed(self, array, vol_name, src_size, vol_size):
        """Extend the volume from size src_size to size vol_size."""
        if vol_size > src_size:
            vol_size = vol_size * units.Gi
            array.patch_volumes(names=[vol_name],
                                volume=flasharray.VolumePatch(
                                    provisioned=vol_size))

    @pure_driver_debug_trace
    def delete_volume(self, volume):
        """Disconnect all hosts and delete the volume"""
        vol_name = self._get_vol_name(volume)
        current_array = self._get_current_array()
        # Do a pass over remaining connections on the current array, if
        # we can try and remove any remote connections too.
        hosts = list(current_array.get_connections(
            volume_names=[vol_name]).items)
        for host_info in range(0, len(hosts)):
            host_name = hosts[host_info].host.name
            self._disconnect_host(current_array, host_name, vol_name)

        # Finally, it should be safe to delete the volume
        res = current_array.patch_volumes(names=[vol_name],
                                          volume=flasharray.VolumePatch(
                                              destroyed=True))
        if self.configuration.pure_eradicate_on_delete:
            current_array.delete_volumes(names=[vol_name])
        if res.status_code == 400:
            with excutils.save_and_reraise_exception() as ctxt:
                if ERR_MSG_NOT_EXIST in res.errors[0].message:
                    # Happens if the volume does not exist.
                    ctxt.reraise = False
                    LOG.warning("Volume deletion failed with message: %s",
                                res.errors[0].message)


    @pure_driver_debug_trace
    def create_snapshot(self, snapshot):
        """Creates a snapshot."""

        # Get current array in case we have failed over via replication.
        current_array = self._get_current_array()
        vol_name, snap_suff = self._get_snap_name(snapshot).split(".")
        volume_snapshot = flasharray.VolumeSnapshotPost(suffix=snap_suff)
        current_array.post_volume_snapshots(source_names=[vol_name],
                                            volume_snapshot=volume_snapshot)
        if not snapshot.metadata:
            snapshot_update = {
                'metadata': {'array_snapshot_name': self._get_snap_name(
                    snapshot),
                    'array_name': self._array.array_name}
            }
        else:
            snapshot_update = {
                'metadata': {**snapshot.metadata,
                             'array_snapshot_name': self._get_snap_name(
                                 snapshot),
                             'array_name': self._array.array_name}
            }
        return snapshot_update

    @pure_driver_debug_trace
    def delete_snapshot(self, snapshot):
        """Deletes a snapshot."""

        # Get current array in case we have failed over via replication.
        current_array = self._get_current_array()

        snap_name = self._get_snap_name(snapshot)
        volume_snap = flasharray.VolumeSnapshotPatch(destroyed=True)
        res = current_array.patch_volume_snapshots(names=[snap_name],
                                                   volume_snapshot=volume_snap)
        if self.configuration.pure_eradicate_on_delete:
            current_array.delete_volume_snapshots(names=[snap_name])
        if res.status_code == 400:
            with excutils.save_and_reraise_exception() as ctxt:
                if (ERR_MSG_NOT_EXIST in res.errors[0].message or
                        ERR_MSG_NO_SUCH_SNAPSHOT in res.errors[0].message or
                        ERR_MSG_PENDING_ERADICATION in res.errors[0].message):
                    # Happens if the snapshot does not exist.
                    ctxt.reraise = False
                    LOG.warning("Unable to delete snapshot, assuming "
                                "already deleted. Error: %s",
                                res.errors[0].message)


    def ensure_export(self, context, volume):
        pass

    def create_export(self, context, volume, connector):
        pass


#check the code later..........................................!!!!!


    def _is_multiattach_to_host(self, volume_attachment, host_name):
        # When multiattach is enabled a volume could be attached to multiple
        # instances which are hosted on the same Nova compute.
        # Because Purity cannot recognize the volume is attached more than
        # one instance we should keep the volume attached to the Nova compute
        # until the volume is detached from the last instance
        if not volume_attachment:
            return False

        attachment = [a for a in volume_attachment
                      if a.attach_status == "attached" and
                      a.attached_host == host_name]
        return len(attachment) > 1

    @pure_driver_debug_trace
    def _disconnect(self, array, volume, connector, remove_remote_hosts=True,
                    is_multiattach=False):
        """Disconnect the volume from the host described by the connector.

        If no connector is specified it will remove *all* attachments for
        the volume.

        Returns True if it was the hosts last connection.
        """
        vol_name = self._get_vol_name(volume)
        if connector is None:
            # If no connector was provided it is a force-detach, remove all
            # host connections for the volume
            LOG.warning("Removing ALL host connections for volume %s",
                        vol_name)
            connections = list(array.get_connections(
                volume_names=[vol_name]).items)
            for connection in range(0, len(connections)):
                self._disconnect_host(array,
                                      connections[connection]['host'],
                                      vol_name)
            return False
        else:
            # Normal case with a specific initiator to detach it from
            hosts = self._get_host(array, connector,
                                   remote=remove_remote_hosts)
            if hosts:
                any_in_use = False
                host_in_use = False
                for host in hosts:
                    host_name = host.name
                    if not is_multiattach:
                        host_in_use = self._disconnect_host(array,
                                                            host_name,
                                                            vol_name)
                    else:
                        LOG.warning("Unable to disconnect host from volume. "
                                    "Volume is multi-attached.")
                    any_in_use = any_in_use or host_in_use
                return any_in_use
            else:
                LOG.error("Unable to disconnect host from volume, could not "
                          "determine Purity host on array %s",
                          array.backend_id)
                return False


    @pure_driver_debug_trace
    def terminate_connection(self, volume, connector, **kwargs):
        """Terminate connection."""
        vol_name = self._get_vol_name(volume)
        # None `connector` indicates force detach, then delete all even
        # if the volume is multi-attached.
        multiattach = (connector is not None and
                       self._is_multiattach_to_host(volume.volume_attachment,
                                                    connector["host"]))
        if self._is_vol_in_pod(vol_name):
            # Try to disconnect from each host, they may not be online though
            # so if they fail don't cause a problem.
            for array in self._uniform_active_cluster_target_arrays:
                res = self._disconnect(array, volume, connector,
                                       remove_remote_hosts=False,
                                       is_multiattach=multiattach)
                if not res:
                    # Swallow any exception, just warn and continue
                    LOG.warning("Disconnect on secondary array failed")
        # Now disconnect from the current array
        self._disconnect(self._get_current_array(), volume,
                         connector, remove_remote_hosts=False,
                         is_multiattach=multiattach)

    @pure_driver_debug_trace
    def _disconnect_host(self, array, host_name, vol_name):
        """Return value indicates if host should be cleaned up."""
        res = array.delete_connections(host_names=[host_name],
                                       volume_names=[vol_name])
        if res.status_code == 400:
            with excutils.save_and_reraise_exception() as ctxt:
                if (ERR_MSG_NOT_EXIST in res.errors[0].message or
                        ERR_MSG_HOST_NOT_EXIST in res.errors[0].message):
                    # Happens if the host and volume are not connected or
                    # the host has already been deleted
                    ctxt.reraise = False
                    LOG.warning("Disconnection failed with message: "
                                "%(msg)s.",
                                {"msg": res.errors[0].message})

        # If it is a remote host, call it quits here. We cannot delete a remote
        # host even if it should be cleaned up now.
        if ':' in host_name:
            return

        connections = None
        res = array.get_connections(host_names=[host_name])
        connection_obj = getattr(res, "items", None)
        if connection_obj:
            connections = list(connection_obj)
        if res.status_code == 400:
            with excutils.save_and_reraise_exception() as ctxt:
                if ERR_MSG_NOT_EXIST in res.errors[0].message:
                    ctxt.reraise = False

        # Assume still used if volumes are attached
        host_still_used = bool(connections)
        if GENERATED_NAME.match(host_name) and not host_still_used:
            LOG.info("Attempting to delete unneeded host %(host_name)r.",
                     {"host_name": host_name})
            res = array.delete_hosts(names=[host_name])
            if res.status_code == 200:
                host_still_used = False
            else:
                with excutils.save_and_reraise_exception() as ctxt:
                    if ERR_MSG_NOT_EXIST in res.errors[0].message:
                        # Happens if the host is already deleted.
                        # This is fine though, just log so we know what
                        # happened.
                        ctxt.reraise = False
                        host_still_used = False
                        LOG.debug("Purity host deletion failed: "
                                  "%(msg)s.", {"msg": res.errors[0].message})
                    if ERR_MSG_EXISTING_CONNECTIONS in res.errors[0].message:
                        # If someone added a connection underneath us
                        # that's ok, just keep going.
                        ctxt.reraise = False
                        host_still_used = True
                        LOG.debug("Purity host deletion ignored: %(msg)s",
                                  {"msg": res.errors[0].message})
        return not host_still_used


    @pure_driver_debug_trace
    def _update_volume_stats(self):
        """Set self._stats with relevant information."""
        current_array = self._get_current_array()
        space_info = list(current_array.get_arrays_space().items)[0]
        perf_info = list(current_array.get_arrays_performance().items)[0]
        hosts = list(current_array.get_hosts().items)
        volumes = list(current_array.get_volumes().items)
        snaps = list(current_array.get_volume_snapshots().items)
        pgroups = list(current_array.get_protection_groups().items)

        # Perform some translations and calculations
        total_capacity = float(space_info.capacity) / units.Gi
        used_space = float(space_info.space.total_physical) / units.Gi
        free_space = float(total_capacity - used_space)
        provisioned_space = float(space_info.space.
                                  total_provisioned) / units.Gi
        total_reduction = float(space_info.space.total_reduction)
        total_vols = len(volumes)
        total_hosts = len(hosts)
        total_snaps = len(snaps)
        total_pgroups = len(pgroups)
        thin_provisioning = self._get_thin_provisioning(total_reduction)

        # Start with some required info
        data = dict(
            volume_backend_name=self._backend_name,
            vendor_name='Pure Storage',
            driver_version=self.VERSION,
            storage_protocol=self._storage_protocol,
        )

        # Add flags for supported features
        data['consistencygroup_support'] = True
        data['thin_provisioning_support'] = True
        data['multiattach'] = True
        data['consistent_group_replication_enabled'] = True
        data['consistent_group_snapshot_enabled'] = True
        data['QoS_support'] = True

        # Add capacity info for scheduler
        data['total_capacity_gb'] = total_capacity
        data['free_capacity_gb'] = free_space
        data['reserved_percentage'] = self.configuration.reserved_percentage
        data['provisioned_capacity'] = provisioned_space
        data['max_over_subscription_ratio'] = thin_provisioning

        # Add the filtering/goodness functions
        data['filter_function'] = self.get_filter_function()
        data['goodness_function'] = self.get_goodness_function()

        # Add array metadata counts for filtering and weighing functions
        data['total_volumes'] = total_vols
        data['total_snapshots'] = total_snaps
        data['total_hosts'] = total_hosts
        data['total_pgroups'] = total_pgroups

        # Add performance stats for filtering and weighing functions
        #  IOPS
        data['writes_per_sec'] = perf_info.writes_per_sec
        data['reads_per_sec'] = perf_info.reads_per_sec

        #  Bandwidth
        data['input_per_sec'] = perf_info.write_bytes_per_sec
        data['output_per_sec'] = perf_info.read_bytes_per_sec

        #  Latency
        data['usec_per_read_op'] = perf_info.usec_per_read_op
        data['usec_per_write_op'] = perf_info.usec_per_write_op
        data['queue_depth'] = getattr(perf_info, 'queue_depth', 0)

        #  Replication
        data["replication_capability"] = self._get_replication_capability()
        data["replication_enabled"] = self._is_replication_enabled
        repl_types = []
        if self._is_replication_enabled:
            repl_types = [REPLICATION_TYPE_ASYNC]
        if self._is_active_cluster_enabled:
            repl_types.append(REPLICATION_TYPE_SYNC)
        if self._is_trisync_enabled:
            repl_types.append(REPLICATION_TYPE_TRISYNC)
        data["replication_type"] = repl_types
        data["replication_count"] = len(self._replication_target_arrays)
        data["replication_targets"] = [array.backend_id for array
                                       in self._replication_target_arrays]
        self._stats = data


    def _get_thin_provisioning(self, total_reduction):
        """Get the current value for the thin provisioning ratio.

        If pure_automatic_max_oversubscription_ratio is True we will calculate
        a value, if not we will respect the configuration option for the
        max_over_subscription_ratio.
        """

        if (self.configuration.pure_automatic_max_oversubscription_ratio and
                total_reduction < 100):
            # If total_reduction is > 100 then this is a very under-utilized
            # array and therefore the oversubscription rate is effectively
            # meaningless.
            # In this case we look to the config option as a starting
            # point. Once some volumes are actually created and some data is
            # stored on the array a much more accurate number will be
            # presented based on current usage.
            thin_provisioning = total_reduction
        else:
            thin_provisioning = volume_utils.get_max_over_subscription_ratio(
                self.configuration.max_over_subscription_ratio,
                supports_auto=True)

        return thin_provisioning

    @pure_driver_debug_trace
    def extend_volume(self, volume, new_size):
        """Extend volume to new_size."""

        # Get current array in case we have failed over via replication.
        current_array = self._get_current_array()

        vol_name = self._get_vol_name(volume)
        new_size = new_size * units.Gi
        current_array.patch_volumes(names=[vol_name],
                                    volume=flasharray.VolumePatch(
                                    provisioned=new_size))

    def _add_volume_to_consistency_group(self, group, vol_name):
        pgroup_name = self._get_pgroup_name(group)
        current_array = self._get_current_array()
        current_array.post_protection_groups_volumes(
            group_names=[pgroup_name],
            member_names=[vol_name])


    def _validate_manage_existing_vol_type(self, volume):
        """Ensure the volume type makes sense for being managed.

        We will not allow volumes that need to be sync-rep'd to be managed.
        There isn't a safe way to automate adding them to the Pod from here,
        an admin doing the import to Cinder would need to handle that part
        first.
        """
        replication_type = self._get_replication_type_from_vol_type(
            volume.volume_type)
        if replication_type == REPLICATION_TYPE_SYNC:
            raise exception.ManageExistingVolumeTypeMismatch(
                _("Unable to managed volume with type requiring sync"
                  " replication enabled."))

    def _validate_manage_existing_ref(self, existing_ref, is_snap=False):
        """Ensure that an existing_ref is valid and return volume info

        If the ref is not valid throw a ManageExistingInvalidReference
        exception with an appropriate error.

        Will return volume or snapshot information from the array for
        the object specified by existing_ref.
        """
        if "name" not in existing_ref or not existing_ref["name"]:
            raise exception.ManageExistingInvalidReference(
                existing_ref=existing_ref,
                reason=_("manage_existing requires a 'name'"
                         " key to identify an existing volume."))

        if is_snap:
            # Purity snapshot names are prefixed with the source volume name.
            ref_vol_name, ref_snap_suffix = existing_ref['name'].split('.')
        else:
            ref_vol_name = existing_ref['name']

        if not is_snap and '::' in ref_vol_name:
            # Don't allow for managing volumes in a pod
            raise exception.ManageExistingInvalidReference(
                _("Unable to manage volume in a Pod"))

        current_array = self._get_current_array()
        volres = current_array.get_volumes(names=[ref_vol_name])
        if volres.status_code == 200:
            volume_info = list(volres.items)[0]
            if volume_info:
                if is_snap:
                    snapres = current_array.get_volume_snapshots(
                        names=[existing_ref['name']])
                    if snapres.status_code == 200:
                        snap = list(snapres.items)[0]
                        return snap
                    else:
                        with excutils.save_and_reraise_exception() as ctxt:
                            if ERR_MSG_NOT_EXIST in volres.errors[0].message:
                                ctxt.reraise = False

                else:
                    return volume_info
        else:
            with excutils.save_and_reraise_exception() as ctxt:
                if ERR_MSG_NOT_EXIST in volres.errors[0].message:
                    ctxt.reraise = False

        # If volume information was unable to be retrieved we need
        # to throw an Invalid Reference exception.
        raise exception.ManageExistingInvalidReference(
            existing_ref=existing_ref,
            reason=_("Unable to find Purity ref with name=%s") % ref_vol_name)


    @pure_driver_debug_trace
    def manage_existing(self, volume, existing_ref):
        """Brings an existing backend storage object under Cinder management.

        We expect a volume name in the existing_ref that matches one in Purity.
        """
        self._validate_manage_existing_vol_type(volume)
        self._validate_manage_existing_ref(existing_ref)

        ref_vol_name = existing_ref['name']
        current_array = self._get_current_array()
        volume_data = list(current_array.get_volumes(
            names=[ref_vol_name]).items)[0]
        connected_hosts = volume_data.connection_count
        if connected_hosts > 0:
            raise exception.ManageExistingInvalidReference(
                existing_ref=existing_ref,
                reason=_("%(driver)s manage_existing cannot manage a volume "
                         "connected to hosts. Please disconnect this volume "
                         "from existing hosts before importing"
                         ) % {'driver': self.__class__.__name__})
        new_vol_name = self._generate_purity_vol_name(volume)
        LOG.info("Renaming existing volume %(ref_name)s to %(new_name)s",
                 {"ref_name": ref_vol_name, "new_name": new_vol_name})
        self._rename_volume_object(ref_vol_name,
                                   new_vol_name,
                                   raise_not_exist=True)
        # If existing volume has QoS settings then clear these out
        vol_iops = getattr(volume_data.qos, "iops_limit", None)
        vol_bw = getattr(volume_data.qos, "bandwidth_limit", None)
        if vol_bw or vol_iops:
            LOG.info("Removing pre-existing QoS settings on managed volume.")
            current_array.patch_volumes(
                names=[new_vol_name],
                volume=flasharray.VolumePatch(
                    qos=flasharray.Qos(iops_limit=100000000,
                                       bandwidth_limit=549755813888)))
        # Check if the volume_type has QoS settings and if so
        # apply them to the newly managed volume
        qos = None
        qos = self._get_qos_settings(volume.volume_type)
        if qos:
            self.set_qos(current_array, new_vol_name, qos)
        volume.provider_id = new_vol_name
        async_enabled = self._enable_async_replication_if_needed(current_array,
                                                                 volume)
        repl_status = fields.ReplicationStatus.DISABLED
        if async_enabled:
            repl_status = fields.ReplicationStatus.ENABLED
        return {
            'provider_id': new_vol_name,
            'replication_status': repl_status,
            'metadata': {'array_volume_name': new_vol_name,
                         'array_name': current_array.array_name},
        }

    @pure_driver_debug_trace
    def manage_existing_get_size(self, volume, existing_ref):
        """Return size of volume to be managed by manage_existing.

        We expect a volume name in the existing_ref that matches one in Purity.
        """
        volume_info = self._validate_manage_existing_ref(existing_ref)
        size = self._round_bytes_to_gib(volume_info.provisioned)

        return size
    
    def _rename_volume_object(self,
                              old_name,
                              new_name,
                              raise_not_exist=False,
                              snapshot=False):
        """Rename a volume object (could be snapshot) in Purity.

        This will not raise an exception if the object does not exist
        """
        current_array = self._get_current_array()
        if snapshot:
            res = current_array.patch_volume_snapshots(
                names=[old_name],
                volume_snapshot=flasharray.VolumePatch(name=new_name))
        else:
            res = current_array.patch_volumes(
                names=[old_name],
                volume=flasharray.VolumePatch(name=new_name))
        if res.status_code == 400:
            with excutils.save_and_reraise_exception() as ctxt:
                if ERR_MSG_NOT_EXIST in res.errors[0].message:
                    ctxt.reraise = raise_not_exist
                    LOG.warning("Unable to rename %(old_name)s, error "
                                "message: %(error)s",
                                {"old_name": old_name,
                                 "error": res.errors[0].message})
        return new_name

    @pure_driver_debug_trace
    def unmanage(self, volume):
        """Removes the specified volume from Cinder management.

        Does not delete the underlying backend storage object.

        The volume will be renamed with "-unmanaged" as a suffix
        """

        vol_name = self._get_vol_name(volume)
        if len(vol_name + UNMANAGED_SUFFIX) > MAX_VOL_LENGTH:
            unmanaged_vol_name = vol_name[:-len(UNMANAGED_SUFFIX)] + \
                UNMANAGED_SUFFIX
        else:
            unmanaged_vol_name = vol_name + UNMANAGED_SUFFIX
        LOG.info("Renaming existing volume %(ref_name)s to %(new_name)s",
                 {"ref_name": vol_name, "new_name": unmanaged_vol_name})
        self._rename_volume_object(vol_name, unmanaged_vol_name)

    def manage_existing_snapshot(self, snapshot, existing_ref):
        """Brings an existing backend storage object under Cinder management.

        We expect a snapshot name in the existing_ref that matches one in
        Purity.
        """
        self._validate_manage_existing_ref(existing_ref, is_snap=True)
        ref_snap_name = existing_ref['name']
        new_snap_name = self._get_snap_name(snapshot)
        LOG.info("Renaming existing snapshot %(ref_name)s to "
                 "%(new_name)s", {"ref_name": ref_snap_name,
                                  "new_name": new_snap_name})
        self._rename_volume_object(ref_snap_name,
                                   new_snap_name,
                                   raise_not_exist=True,
                                   snapshot=True)
        return {
            'metadata': {'array_snapshot_name': new_snap_name,
                         'array_name': self._array.array_name},
        }

    def manage_existing_snapshot_get_size(self, snapshot, existing_ref):
        """Return size of snapshot to be managed by manage_existing.

        We expect a snapshot name in the existing_ref that matches one in
        Purity.
        """
        snap_info = self._validate_manage_existing_ref(existing_ref,
                                                       is_snap=True)
        size = self._round_bytes_to_gib(snap_info.provisioned)
        return size

    def unmanage_snapshot(self, snapshot):
        """Removes the specified snapshot from Cinder management.

        Does not delete the underlying backend storage object.

        We expect a snapshot name in the existing_ref that matches one in
        Purity.
        """
        snap_name = self._get_snap_name(snapshot)
        if len(snap_name + UNMANAGED_SUFFIX) > MAX_SNAP_LENGTH:
            unmanaged_snap_name = snap_name[:-len(UNMANAGED_SUFFIX)] + \
                UNMANAGED_SUFFIX
        else:
            unmanaged_snap_name = snap_name + UNMANAGED_SUFFIX
        LOG.info("Renaming existing snapshot %(ref_name)s to "
                 "%(new_name)s", {"ref_name": snap_name,
                                  "new_name": unmanaged_snap_name})
        self._rename_volume_object(snap_name,
                                   unmanaged_snap_name,
                                   snapshot=True)

    def get_manageable_volumes(self, cinder_volumes, marker, limit, offset,
                               sort_keys, sort_dirs):
        """List volumes on the backend available for management by Cinder.

        Rule out volumes that are attached to a Purity host or that
        are already in the list of cinder_volumes.

        Also exclude any volumes that are in a pod, it is difficult to safely
        move in/out of pods from here without more context so we'll rely on
        the admin to move them before managing the volume.

        We return references of the volume names for any others.
        """
        array = self._get_current_array()
        pure_vols = list(array.get_volumes().items)
        connections = list(array.get_connections().items)

        # Put together a map of volumes that are connected to hosts
        connected_vols = {}
        for connect in range(0, len(connections)):
            connected_vols[connections[connect].volume.name] = \
                connections[connect].host.name

        # Put together a map of existing cinder volumes on the array
        # so we can lookup cinder id's by purity volume names
        existing_vols = {}
        for cinder_vol in cinder_volumes:
            existing_vols[self._get_vol_name(cinder_vol)] = cinder_vol.name_id

        manageable_vols = []
        for pure_vol in range(0, len(pure_vols)):
            vol_name = pure_vols[pure_vol].name
            cinder_id = existing_vols.get(vol_name)
            not_safe_msgs = []
            host = connected_vols.get(vol_name)
            in_pod = ("::" in vol_name)
            is_deleted = pure_vols[pure_vol].destroyed

            if host:
                not_safe_msgs.append(_('Volume connected to host %s') % host)

            if cinder_id:
                not_safe_msgs.append(_('Volume already managed'))

            if in_pod:
                not_safe_msgs.append(_('Volume is in a Pod'))

            if is_deleted:
                not_safe_msgs.append(_('Volume is deleted'))

            is_safe = (len(not_safe_msgs) == 0)
            reason_not_safe = ''
            if not is_safe:
                for i, msg in enumerate(not_safe_msgs):
                    if i > 0:
                        reason_not_safe += ' && '
                    reason_not_safe += "%s" % msg

            manageable_vols.append({
                'reference': {'name': vol_name},
                'size': self._round_bytes_to_gib(
                    pure_vols[pure_vol].provisioned),
                'safe_to_manage': is_safe,
                'reason_not_safe': reason_not_safe,
                'cinder_id': cinder_id,
                'extra_info': None,
            })

        return volume_utils.paginate_entries_list(
            manageable_vols, marker, limit, offset, sort_keys, sort_dirs)

    def get_manageable_snapshots(self, cinder_snapshots, marker, limit, offset,
                                 sort_keys, sort_dirs):
        """List snapshots on the backend available for management by Cinder."""
        array = self._get_current_array()
        pure_snapshots = list(array.get_volume_snapshots().items)
        # Put together a map of existing cinder snapshots on the array
        # so we can lookup cinder id's by purity snapshot names
        existing_snapshots = {}
        for cinder_snap in cinder_snapshots:
            name = self._get_snap_name(cinder_snap)
            existing_snapshots[name] = cinder_snap.id

        manageable_snaps = []
        for pure_snap in range(0, len(pure_snapshots)):
            snap_name = pure_snapshots[pure_snap].name
            cinder_id = existing_snapshots.get(snap_name)

            is_safe = True
            reason_not_safe = None

            if cinder_id:
                is_safe = False
                reason_not_safe = _("Snapshot already managed.")

            if pure_snapshots[pure_snap].destroyed:
                is_safe = False
                reason_not_safe = _("Snapshot is deleted.")

            manageable_snaps.append({
                'reference': {'name': snap_name},
                'size': self._round_bytes_to_gib(
                    pure_snapshots[pure_snap].provisioned),
                'safe_to_manage': is_safe,
                'reason_not_safe': reason_not_safe,
                'cinder_id': cinder_id,
                'extra_info': None,
                'source_reference': {
                    'name': getattr(pure_snapshots[pure_snap].source,
                                    "name", None)},
            })

        return volume_utils.paginate_entries_list(
            manageable_snaps, marker, limit, offset, sort_keys, sort_dirs)



    @staticmethod
    def _round_bytes_to_gib(size):
        return int(math.ceil(float(size) / units.Gi))

    def _get_flasharray(self, san_ip, api_token, rest_version=None,
                        verify_ssl=None, ssl_cert_path=None):

        try:
            array = flasharray.Client(target=san_ip,
                                      api_token=api_token,
                                      verify_ssl=verify_ssl,
                                      ssl_cert=ssl_cert_path,
                                      user_agent=self._user_agent,
                                      )
        except Exception:
            return None
        array_info = list(array.get_arrays().items)[0]
        array.array_name = array_info.name
        array.array_id = array_info.id
        array._rest_version = array.get_rest_version()

        # Configure some extra tracing on requests made to the array
        if hasattr(array, '_request'):
            def trace_request(fn):
                def wrapper(*args, **kwargs):
                    request_id = uuid.uuid4().hex
                    LOG.debug("Making HTTP Request [%(id)s]:"
                              " 'args=%(args)s kwargs=%(kwargs)s'",
                              {
                                  "id": request_id,
                                  "args": args,
                                  "kwargs": kwargs,
                              })
                    ret = fn(*args, **kwargs)
                    LOG.debug(
                        "Response for HTTP request [%(id)s]: '%(response)s'",
                        {
                            "id": request_id,
                            "response": ret,
                        }
                    )
                    return ret
                return wrapper
            array._request = trace_request(array._request)

        LOG.debug("connected to %(array_name)s with REST API %(api_version)s",
                  {"array_name": array.array_name,
                   "api_version": array._rest_version})
        return array


    @staticmethod
    def _get_pod_for_volume(volume_name):
        """Return the Purity pod name for the given volume.

        This works on the assumption that volume names are always prefixed
        with the pod name followed by '::'
        """
        if '::' not in volume_name:
            # Not in a pod
            return None
        parts = volume_name.split('::')
        if len(parts) != 2 or not parts[0]:
            # Can't parse this.. Should never happen though, would mean a
            # break to the API contract with Purity.
            raise PureDriverException(
                _("Unable to determine pod for volume %s") % volume_name)
        return parts[0]

    @classmethod
    def _is_vol_in_pod(cls, pure_vol_name):
        return bool(cls._get_pod_for_volume(pure_vol_name) is not None)


    def _generate_purity_vol_name(self, volume):
        """Return the name of the volume Purity will use.

        This expects to be given a Volume OVO and not a volume
        dictionary.
        """
        base_name = volume.name

        # Some OpenStack deployments, eg PowerVC, create a volume.name that
        # when appended with our '-cinder' string will exceed the maximum
        # volume name length for Pure, so here we left truncate the true volume
        # name before the opennstack volume_name_template affected it and
        # then put back the template format
        if len(base_name) > 56:
            actual_name = base_name[(len(CONF.volume_name_template) - 2):]
            base_name = CONF.volume_name_template % \
                actual_name[-(56 - len(CONF.volume_name_template)):]

        repl_type = self._get_replication_type_from_vol_type(
            volume.volume_type)
        if repl_type in [REPLICATION_TYPE_SYNC, REPLICATION_TYPE_TRISYNC]:
            base_name = self._replication_pod_name + "::" + base_name

        return base_name + "-cinder"

    def _get_vol_name(self, volume):
        """Return the name of the volume Purity will use."""
        # Use the dictionary access style for compatibility, this works for
        # db or OVO volume objects too.
        return volume['provider_id']

    def _get_snap_name(self, snapshot):
        """Return the name of the snapshot that Purity will use."""
        return "%s.%s" % (self._get_vol_name(snapshot.volume),
                          snapshot["name"])
    
    @staticmethod
    def _get_pgroup_vol_snap_name(pg_name, pgsnap_suffix, volume_name):
        if "::" in volume_name:
            volume_name = volume_name.split("::")[1]
        return "%(pgroup_name)s.%(pgsnap_suffix)s.%(volume_name)s" % {
            'pgroup_name': pg_name,
            'pgsnap_suffix': pgsnap_suffix,
            'volume_name': volume_name,
        }


    @staticmethod
    def _get_pgroup_snap_suffix(group_snapshot):
        return "cgsnapshot-%s-cinder" % group_snapshot['id']

    @staticmethod
    def _get_group_id_from_snap(group_snap):
        # We don't really care what kind of group it is, if we are calling
        # this look for a group_id and fall back to using a consistencygroup_id
        id = None
        try:
            id = group_snap['group_id']
        except AttributeError:
            pass
        if id is None:
            try:
                id = group_snap['consistencygroup_id']
            except AttributeError:
                pass
        return id


    @staticmethod
    def _generate_purity_host_name(name):
        """Return a valid Purity host name based on the name passed in."""
        if len(name) > 23:
            name = name[0:23]
        name = INVALID_CHARACTERS.sub("-", name)
        name = name.lstrip("-")
        return "{name}-{uuid}-cinder".format(name=name, uuid=uuid.uuid4().hex)

    @staticmethod
    def _connect_host_to_vol(array, host_name, vol_name):
        connection = None
        LOG.debug("Connecting volume %(vol)s to host %(host)s.",
                  {"vol": vol_name,
                   "host": host_name})
        res = array.post_connections(
            host_names=[host_name],
            volume_names=[vol_name])
        connection_obj = getattr(res, "items", None)
        if connection_obj:
            connection = list(connection_obj)
        if res.status_code == 400:
            if ERR_MSG_HOST_NOT_EXIST in res.errors[0].message:
                LOG.debug(
                    'Unable to attach volume to host: %s',
                    res.errors[0].context
                )
                raise PureRetryableException()
            with excutils.save_and_reraise_exception() as ctxt:
                ctxt.reraise = False
                if (res.status_code == 400 and
                        ERR_MSG_ALREADY_EXISTS in res.errors[0].message):
                    # Happens if the volume is already connected to the host.
                    # Treat this as a success.
                    LOG.debug("Volume connection already exists for Purity "
                              "host with message: %s", res.errors[0].message)

                    vol_data = list(array.get_volumes(names=[vol_name]).items)
                    vol_id = vol_data[0].id
                    connected_host = list(
                        array.get_connections(
                            volume_names=[vol_name], host_names=[host_name]
                        ).items
                    )[0]
                    connection = [
                        {
                            "host": {"name": host_name},
                            "host_group": {},
                            'protocol_endpoint': {},
                            "volume": {"name": vol_name, "id": vol_id},
                            "lun": getattr(connected_host, "lun", None),
                            "nsid": getattr(connected_host, "nsid", None),
                        }
                    ]
        if not connection:
            raise PureDriverException(
                reason=_("Unable to connect or find connection to host"))

        return connection

    @pure_driver_debug_trace
    def set_personality(self, array, host_name, personality):
        res = array.patch_hosts(names=[host_name],
                                host=flasharray.HostPatch(
                                    personality=personality))
        if res.status_code == 400:
            if ERR_MSG_HOST_NOT_EXIST in res.errors[0].message:
                # If the host disappeared out from under us that's
                # ok, we will just retry and snag a new host.
                LOG.debug('Unable to set host personality: %s',
                          res.errors[0].message)
                raise PureRetryableException()
        return
    

    def _get_wwn(self, pure_vol_name):
        """Return the WWN based on the volume's serial number

        The WWN is composed of the constant '36', the OUI for Pure, followed
        by '0', and finally the serial number.
        """
        array = self._get_current_array()
        volume_info = list(array.get_volumes(names=[pure_vol_name]).items)[0]
        wwn = '3624a9370' + volume_info.serial
        return wwn.lower()

    def _get_current_array(self, init=False):
        if (not init and
                self._is_active_cluster_enabled and
                not self._failed_over_primary_array):
            res = self._array.get_pods(names=[self._replication_pod_name])
            if res.status_code == 200:
                pod_info = list(res.items)[0]
                for target_array in self._active_cluster_target_arrays:
                    LOG.info("Checking target array %s...",
                             target_array.array_name)
                    status_ok = False
                    for pod_array in range(0, len(pod_info.arrays)):
                        if pod_info.arrays[pod_array].id == \
                                target_array.array_id:
                            if pod_info.arrays[pod_array].status == \
                                    'online':
                                status_ok = True
                            break
                    if not status_ok:
                        LOG.warning("Target array is offline. Volume "
                                    "replication in unknown state. Check "
                                    "replication links and array state.")
            else:
                LOG.warning("self.get_pod failed with"
                            " message: %(msg)s",
                            {"msg": res.errors[0].message})
                raise PureDriverException(
                    reason=_("No functional arrays available"))

        return self._array

    def _set_current_array(self, array):
        self._array = array

    @pure_driver_debug_trace
    def _get_valid_ports(self, array):
        ports = []
        res = array.get_controllers(filter="status='ready'")
        if res.status_code != 200:
            with excutils.save_and_reraise_exception() as ctxt:
                ctxt.reraise = False
                LOG.warning("No live controllers found: %s", res.errors[0])
                return ports
        else:
            live_controllers = list(res.items)
        if len(live_controllers) != 0:
            controllers = [controller.name for controller in live_controllers]
            for controller in controllers:
                ports += list(
                    array.get_ports(filter="name='" + controller + ".*'").items
                )
        return ports
