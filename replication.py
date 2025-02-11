import uuid
from cinder.volume import volume_utils
from oslo_utils import excutils
from cinder.objects import volume_type
from cinder.i18n import _
from cinder import utils
from packaging import version
from base_driver import PureBaseVolumeDriver
from oslo_log import log as logging
from oslo_utils import strutils
from constants import *
from .exceptions import *
from cinder.objects import fields
try:
    from pypureclient import flasharray
except ImportError:
    flasharray = None

from.utils import pure_driver_debug_trace


LOG = logging.getLogger(__name__)

class PureReplicationMixin(PureBaseVolumeDriver):
    """Handles replication settings for Pure Storage."""

    def parse_replication_configs(self):
        """Parses replication-related configurations from settings."""
        self._trisync_pg_name = self.configuration.pure_trisync_pg_name
        self._replication_pg_name = self.configuration.pure_replication_pg_name
        self._replication_pod_name = self.configuration.pure_replication_pod_name
        self._replication_interval = self.configuration.pure_replica_interval_default
        self._replication_retention_short_term = self.configuration.pure_replica_retention_short_term_default
        self._replication_retention_long_term = self.configuration.pure_replica_retention_long_term_default
        self._replication_retention_long_term_per_day = self.configuration.pure_replica_retention_long_term_per_day_default
        self._async_replication_retention_policy = self._generate_replication_retention()

        replication_devices = self.configuration.safe_get('replication_device')

        if replication_devices:
            for replication_device in replication_devices:
                backend_id = replication_device["backend_id"]
                san_ip = replication_device["san_ip"]
                api_token = replication_device["api_token"]
                verify_ssl = strutils.bool_from_string(
                    replication_device.get("ssl_cert_verify", False))
                ssl_cert_path = replication_device.get("ssl_cert_path", None)
                repl_type = replication_device.get("type", REPLICATION_TYPE_ASYNC)
                uniform = strutils.bool_from_string(replication_device.get("uniform", False))

                target_array = self._get_flasharray(
                    san_ip,
                    api_token,
                    verify_ssl=verify_ssl,
                    ssl_cert_path=ssl_cert_path
                )
                if target_array:
                    target_array_info = list(target_array.get_arrays().items)[0]
                    target_array.array_name = target_array_info.name
                    target_array.array_id = target_array_info.id
                    target_array.replication_type = repl_type
                    target_array.backend_id = backend_id
                    target_array.uniform = uniform

                    LOG.info("Added secondary array: backend_id='%s',"
                             " name='%s', id='%s', type='%s', uniform='%s'",
                             target_array.backend_id,
                             target_array.array_name,
                             target_array.array_id,
                             target_array.replication_type,
                             target_array.uniform)
                else:
                    LOG.warning("Failed to set up secondary array: %(ip)s",
                                {"ip": san_ip})
                    continue

                self._replication_target_arrays.append(target_array)
                if repl_type == REPLICATION_TYPE_SYNC:
                    self._active_cluster_target_arrays.append(target_array)
                    if target_array.uniform:
                        self._uniform_active_cluster_target_arrays.append(target_array)


    def do_setup_trisync(self):
        repl_device = {}
        async_target = []
        count = 0
        replication_devices = self.configuration.safe_get(
            'replication_device')
        if not replication_devices or len(replication_devices) != 2:
            LOG.error("Unable to configure TriSync Replication. Incorrect "
                      "number of replication devices enabled. "
                      "Only 2 are supported.")
        else:
            for replication_device in replication_devices:
                san_ip = replication_device["san_ip"]
                api_token = replication_device["api_token"]
                repl_type = replication_device.get(
                    "type", REPLICATION_TYPE_ASYNC)
                repl_device[count] = {
                    "rep_type": repl_type,
                    "token": api_token,
                    "san_ip": san_ip,
                }
                count += 1
            if (repl_device[0]["rep_type"] == repl_device[1]["rep_type"]) or (
                    (repl_device[0]["token"] == repl_device[1]["token"])
            ):
                LOG.error("Replication devices provided must be one each "
                          "of sync and async and targets must be different "
                          "to enable TriSync Replication.")
                return
            for replication_device in replication_devices:
                repl_type = replication_device.get(
                    "type", REPLICATION_TYPE_ASYNC)
                if repl_type == "async":
                    san_ip = replication_device["san_ip"]
                    api_token = replication_device["api_token"]
                    verify_ssl = strutils.bool_from_string(
                        replication_device.get("ssl_cert_verify", False))
                    ssl_cert_path = replication_device.get(
                        "ssl_cert_path", None)
                    target_array = self._get_flasharray(
                        san_ip,
                        api_token,
                        verify_ssl=verify_ssl,
                        ssl_cert_path=ssl_cert_path
                    )
                    trisync_async_info = list(
                        target_array.get_arrays().items)[0]
                    target_array.array_name = trisync_async_info.name

                    async_target.append(target_array)

            self._trisync_name = self._replication_pod_name + \
                "::" + \
                self._trisync_pg_name
            self._is_trisync_enabled = True
            self._setup_replicated_pgroups(
                self._get_current_array(),
                async_target,
                self._trisync_name,
                self._replication_interval,
                self._async_replication_retention_policy
            )

    def do_setup_replication(self):
        replication_devices = self.configuration.safe_get(
            'replication_device')
        if replication_devices:
            self.parse_replication_configs()
            self._is_replication_enabled = True

            if len(self._active_cluster_target_arrays) > 0:
                self._is_active_cluster_enabled = True

                # Only set this up on sync rep arrays
                self._setup_replicated_pods(
                    self._get_current_array(True),
                    self._active_cluster_target_arrays,
                    self._replication_pod_name
                )

            # Even if the array is configured for sync rep set it
            # up to handle async too
            self._setup_replicated_pgroups(
                self._get_current_array(True),
                self._replication_target_arrays,
                self._replication_pg_name,
                self._replication_interval,
                self._async_replication_retention_policy
            )

    def _setup_volume(self, array, volume, purity_vol_name):
        # set provider_id early so other methods can use it even though
        # it wont be set in the cinder DB until we return from create_volume
        volume.provider_id = purity_vol_name
        async_enabled = False
        trisync_enabled = False
        self._add_to_group_if_needed(volume, purity_vol_name)
        async_enabled = self._enable_async_replication_if_needed(
            array, volume)
        trisync_enabled = self._enable_trisync_replication_if_needed(
            array, volume)
        repl_type = self._get_replication_type_from_vol_type(
            volume.volume_type)
        try:
            pgroup = array.get_protection_groups_volumes(
                member_names=[volume.provider_id]).items
        except AttributeError:
            # AttributeError from pypureclient SDK as volume
            # not in a protection group
            pgroup = None
        if (repl_type in [REPLICATION_TYPE_ASYNC, REPLICATION_TYPE_TRISYNC] and
                not pgroup):
            LOG.error("Failed to add volume %s to pgroup, removing volume")
            array.patch_volumes(names=[purity_vol_name],
                                volume=flasharray.VolumePatch(
                                    destroyed=True))
            array.delete_volumes(names=[purity_vol_name])

        repl_status = fields.ReplicationStatus.DISABLED
        if (self._is_vol_in_pod(purity_vol_name) or
                (async_enabled or trisync_enabled)):
            repl_status = fields.ReplicationStatus.ENABLED

        if not volume.metadata:
            model_update = {
                'id': volume.id,
                'provider_id': purity_vol_name,
                'replication_status': repl_status,
                'metadata': {'array_volume_name': purity_vol_name,
                             'array_name': self._array.array_name}
            }
        else:
            model_update = {
                'id': volume.id,
                'provider_id': purity_vol_name,
                'replication_status': repl_status,
                'metadata': {**volume.metadata,
                             'array_volume_name': purity_vol_name,
                             'array_name': self._array.array_name}
            }
        return model_update
    
    def _enable_async_replication_if_needed(self, array, volume):
        repl_type = self._get_replication_type_from_vol_type(
            volume.volume_type)
        if repl_type == REPLICATION_TYPE_ASYNC:
            self._enable_async_replication(array, volume)
            return True
        return False
    
    def _enable_trisync_replication_if_needed(self, array, volume):
        repl_type = self._get_replication_type_from_vol_type(
            volume.volume_type)
        if (self.configuration.pure_trisync_enabled and
                repl_type == REPLICATION_TYPE_TRISYNC):
            self._enable_trisync_replication(array, volume)
            return True
        return False

    def _enable_trisync_replication(self, array, volume):
        """Add volume to sync-replicated protection group"""
        array.post_protection_groups_volumes(
            group_names=[self._trisync_name],
            member_names=[self._get_vol_name(volume)])

    def _disable_trisync_replication(self, array, volume):
        """Remove volume from sync-replicated protection group"""
        array.delete_protection_groups_volumes(
            group_names=[self._trisync_name],
            member_names=[self._get_vol_name(volume)])

    def _enable_async_replication(self, array, volume):
        """Add volume to replicated protection group."""
        array.post_protection_groups_volumes(
            group_names=[self._replication_pg_name],
            member_names=[self._get_vol_name(volume)])


    def _get_replication_capability(self):
        """Discovered connected arrays status for replication"""
        connections = list(
            self._get_current_array().get_array_connections().items)
        is_sync, is_async, is_trisync = False, False, False
        for conn in range(0, len(connections)):
            # If connection status is connected, we can have
            # either sync or async replication
            if connections[conn].status == "connected":
                # check for async replication
                if connections[conn].type == "async-replication":
                    is_async = True
                # check for sync replication
                elif connections[conn].type == "sync-replication":
                    is_sync = True
            # If we've connections for both sync and async
            # replication, we can set trisync replication
            # and exit the loop
            if is_sync and is_async:
                is_trisync = True
                break
        # Check if it is a trisync replication
        if is_trisync:
            replication_type = "trisync"
        # If replication is not trisync, it will be either
        # sync or async
        elif is_sync:
            replication_type = "sync"
        elif is_async:
            replication_type = "async"
        else:
            replication_type = None
        return replication_type
    

    @pure_driver_debug_trace
    def create_consistencygroup(self, context, group, grp_type=None):
        """Creates a consistencygroup."""

        current_array = self._get_current_array()
        group_name = self._get_pgroup_name(group)
        LOG.debug('Creating Consistency Group %(group_name)s',
                  {'group_name': group_name})
        current_array.post_protection_groups(
            names=[group_name])
        if grp_type:
            current_array.patch_protection_groups(
                names=[group_name],
                protection_group=flasharray.ProtectionGroup(
                    replication_schedule=flasharray.ReplicationSchedule(
                        frequency=self._replication_interval)))
            for target_array in self._replication_target_arrays:
                # Configure PG to replicate to target_array.
                current_array.post_protection_groups_targets(
                    group_names=[group_name],
                    member_names=[target_array.array_name])
                # Wait until "Target Group" setting propagates to target_array.
                pgroup_name_on_target = self._get_pgroup_name_on_target(
                    current_array.array_name, group_name)

                if grp_type == REPLICATION_TYPE_TRISYNC:
                    pgroup_name_on_target = group_name.replace("::", ":")

                target_array.patch_protection_groups_targets(
                    group_names=[pgroup_name_on_target],
                    target=flasharray.TargetProtectionGroupPostPatch(
                        allowed=True))

                # Wait until source array acknowledges previous operation.
                self._wait_until_source_array_allowed(current_array,
                                                      group_name)
                # Start replication on the PG.
                current_array.patch_protection_groups(
                    names=[group_name],
                    protection_group=flasharray.ProtectionGroup(
                        replication_schedule=flasharray.ReplicationSchedule(
                            enabled=True)))

        model_update = {'status': fields.ConsistencyGroupStatus.AVAILABLE}
        return model_update

    def _create_cg_from_cgsnap(self, volumes, snapshots):
        """Creates a new consistency group from a cgsnapshot.

        The new volumes will be consistent with the snapshot.
        """
        vol_models = []
        for volume, snapshot in zip(volumes, snapshots):
            vol_models.append(self.create_volume_from_snapshot(
                volume,
                snapshot,
                cgsnapshot=True))
        return vol_models

    def _create_cg_from_cg(self, group, source_group, volumes, source_vols):
        """Creates a new consistency group from an existing cg.

        The new volumes will be in a consistent state, but this requires
        taking a new temporary group snapshot and cloning from that.
        """
        vol_models = []
        pgroup_name = self._get_pgroup_name(source_group)
        tmp_suffix = '%s-tmp' % uuid.uuid4()
        tmp_pgsnap_name = '%(pgroup_name)s.%(pgsnap_suffix)s' % {
            'pgroup_name': pgroup_name,
            'pgsnap_suffix': tmp_suffix,
        }
        LOG.debug('Creating temporary Protection Group snapshot %(snap_name)s '
                  'while cloning Consistency Group %(source_group)s.',
                  {'snap_name': tmp_pgsnap_name,
                   'source_group': source_group.id})
        current_array = self._get_current_array()
        suffix = flasharray.ProtectionGroupSnapshotPost(suffix=tmp_suffix)
        current_array.post_protection_group_snapshots(
            source_names=[pgroup_name],
            protection_group_snapshot=suffix)
        volumes, _ = self.update_provider_info(volumes, None)
        try:
            for source_vol, cloned_vol in zip(source_vols, volumes):
                vol_models.append(cloned_vol)
                source_snap_name = self._get_pgroup_vol_snap_name(
                    pgroup_name,
                    tmp_suffix,
                    self._get_vol_name(source_vol)
                )
                cloned_vol_name = self._get_vol_name(cloned_vol)
                current_array.post_volumes(names=[cloned_vol_name],
                                           volume=flasharray.VolumePost(
                                           source=flasharray.Reference(
                                               name=source_snap_name)))
                self._add_volume_to_consistency_group(
                    group,
                    cloned_vol_name
                )
                repl_type = self._get_replication_type_from_vol_type(
                    source_vol.volume_type)
                if (self.configuration.pure_trisync_enabled and
                        repl_type == REPLICATION_TYPE_TRISYNC):
                    self._enable_trisync_replication(current_array, cloned_vol)
                    LOG.info('Trisync replication set for new cloned '
                             'volume %s', cloned_vol_name)

        finally:
            self._delete_pgsnapshot(tmp_pgsnap_name)
        return vol_models


    @pure_driver_debug_trace
    def create_consistencygroup_from_src(self, context, group, volumes,
                                         cgsnapshot=None, snapshots=None,
                                         source_cg=None, source_vols=None,
                                         group_type=None):
        # let generic volume group support handle non-cgsnapshots
        if not volume_utils.is_group_a_cg_snapshot_type(group):
            raise NotImplementedError()
        model_update = self.create_consistencygroup(context, group, group_type)
        if cgsnapshot and snapshots:
            vol_models = self._create_cg_from_cgsnap(volumes,
                                                     snapshots)
        elif source_cg:
            vol_models = self._create_cg_from_cg(group, source_cg,
                                                 volumes, source_vols)

        return model_update, vol_models

    @pure_driver_debug_trace
    def delete_consistencygroup(self, context, group, volumes):
        """Deletes a consistency group."""

        # let generic volume group support handle non-cgsnapshots
        if not volume_utils.is_group_a_cg_snapshot_type(group):
            raise NotImplementedError()
        pgroup_name = self._get_pgroup_name(group)
        current_array = self._get_current_array()
        pgres = current_array.patch_protection_groups(
            names=[pgroup_name],
            protection_group=flasharray.ProtectionGroup(
                destroyed=True))
        if pgres.status_code == 200:
            if self.configuration.pure_eradicate_on_delete:
                current_array.delete_protection_groups(
                    names=[pgroup_name])
        else:
            with excutils.save_and_reraise_exception() as ctxt:
                if (ERR_MSG_PENDING_ERADICATION in pgres.errors[0].message or
                        ERR_MSG_NOT_EXIST in pgres.errors[0].message):
                    # Treat these as a "success" case since we are trying
                    # to delete them anyway.
                    ctxt.reraise = False
                    LOG.warning("Unable to delete Protection Group: %s",
                                pgres.errors[0].context)

        for volume in volumes:
            self.delete_volume(volume)

        return None, None

    @pure_driver_debug_trace
    def update_consistencygroup(self, context, group,
                                add_volumes=None, remove_volumes=None):

        pgroup_name = self._get_pgroup_name(group)
        if add_volumes:
            addvollist = [self._get_vol_name(vol) for vol in add_volumes]
        else:
            addvollist = []

        if remove_volumes:
            remvollist = [self._get_vol_name(vol) for vol in remove_volumes]
        else:
            remvollist = []

        current_array = self._get_current_array()
        current_array.post_protection_groups_volumes(
            group_names=[pgroup_name],
            member_names=addvollist)
        current_array.delete_protection_groups_volumes(
            group_names=[pgroup_name],
            member_names=remvollist)

        return None, None, None


    @pure_driver_debug_trace
    def create_cgsnapshot(self, context, cgsnapshot, snapshots):
        """Creates a cgsnapshot."""

        pgroup_name = self._get_pgroup_name(cgsnapshot.group)
        pgsnap_suffix = self._get_pgroup_snap_suffix(cgsnapshot)
        current_array = self._get_current_array()
        suffix = flasharray.ProtectionGroupSnapshotPost(suffix=pgsnap_suffix)
        current_array.post_protection_group_snapshots(
            source_names=[pgroup_name],
            protection_group_snapshot=suffix)

        return None, None

    def _delete_pgsnapshot(self, pgsnap_name):
        current_array = self._get_current_array()
        pg_snapshot = flasharray.ProtectionGroupSnapshotPatch(destroyed=True)
        res = current_array.patch_protection_group_snapshots(
            protection_group_snapshot=pg_snapshot,
            names=[pgsnap_name])
        if self.configuration.pure_eradicate_on_delete:
            current_array.delete_protection_group_snapshots(
                names=[pgsnap_name])
        if res.status_code == 400:
            with excutils.save_and_reraise_exception() as ctxt:
                if (ERR_MSG_PENDING_ERADICATION in res.errors[0].message or
                        ERR_MSG_NOT_EXIST in res.errors[0].message):
                    # Treat these as a "success" case since we are trying
                    # to delete them anyway.
                    ctxt.reraise = False
                    LOG.warning("Unable to delete Protection Group "
                                "Snapshot: %s", res.errors[0].message)

    @pure_driver_debug_trace
    def delete_cgsnapshot(self, context, cgsnapshot, snapshots):
        """Deletes a cgsnapshot."""

        pgsnap_name = self._get_pgroup_snap_name(cgsnapshot)
        self._delete_pgsnapshot(pgsnap_name)

        return None, None

    def _add_to_group_if_needed(self, volume, vol_name):
        if volume['group_id']:
            if volume_utils.is_group_a_cg_snapshot_type(volume.group):
                self._add_volume_to_consistency_group(
                    volume.group,
                    vol_name
                )
        elif volume['consistencygroup_id']:
            self._add_volume_to_consistency_group(
                volume.consistencygroup,
                vol_name
            )

    def create_group(self, ctxt, group):
        """Creates a group.

        :param ctxt: the context of the caller.
        :param group: the Group object of the group to be created.
        :returns: model_update
        """
        cgr_type = None
        repl_type = None
        if volume_utils.is_group_a_cg_snapshot_type(group):
            if volume_utils.is_group_a_type(
                    group, "consistent_group_replication_enabled"):
                if not self._is_replication_enabled:
                    msg = _("Replication not properly configured on backend.")
                    LOG.error(msg)
                    raise PureDriverException(msg)
                for vol_type_id in group.volume_type_ids:
                    vol_type = volume_type.VolumeType.get_by_name_or_id(
                        ctxt,
                        vol_type_id)
                    repl_type = self._get_replication_type_from_vol_type(
                        vol_type)
                    if repl_type not in [REPLICATION_TYPE_ASYNC,
                                         REPLICATION_TYPE_TRISYNC]:
                        # Unsupported configuration
                        LOG.error("Unable to create group: create consistent "
                                  "replication group with non-replicated or "
                                  "sync replicated volume type is not "
                                  "supported.")
                        model_update = {'status': fields.GroupStatus.ERROR}
                        return model_update
                    if not cgr_type:
                        cgr_type = repl_type
                    elif cgr_type != repl_type:
                        LOG.error("Unable to create group: create consistent "
                                  "replication group with different "
                                  "replication types is not supported.")
                        model_update = {'status': fields.GroupStatus.ERROR}
                        return model_update
            return self.create_consistencygroup(ctxt, group, cgr_type)

        # If it wasn't a consistency group request ignore it and we'll rely on
        # the generic group implementation.
        raise NotImplementedError()

    def delete_group(self, ctxt, group, volumes):
        """Deletes a group.

        :param ctxt: the context of the caller.
        :param group: the Group object of the group to be deleted.
        :param volumes: a list of Volume objects in the group.
        :returns: model_update, volumes_model_update
        """
        if volume_utils.is_group_a_cg_snapshot_type(group):
            return self.delete_consistencygroup(ctxt, group, volumes)

        # If it wasn't a consistency group request ignore it and we'll rely on
        # the generic group implementation.
        raise NotImplementedError()


    def update_group(self, ctxt, group,
                     add_volumes=None, remove_volumes=None):
        """Updates a group.

        :param ctxt: the context of the caller.
        :param group: the Group object of the group to be updated.
        :param add_volumes: a list of Volume objects to be added.
        :param remove_volumes: a list of Volume objects to be removed.
        :returns: model_update, add_volumes_update, remove_volumes_update
        """

        if volume_utils.is_group_a_cg_snapshot_type(group):
            return self.update_consistencygroup(ctxt,
                                                group,
                                                add_volumes,
                                                remove_volumes)

        # If it wasn't a consistency group request ignore it and we'll rely on
        # the generic group implementation.
        raise NotImplementedError()

    def create_group_from_src(self, ctxt, group, volumes,
                              group_snapshot=None, snapshots=None,
                              source_group=None, source_vols=None):
        """Creates a group from source.

        :param ctxt: the context of the caller.
        :param group: the Group object to be created.
        :param volumes: a list of Volume objects in the group.
        :param group_snapshot: the GroupSnapshot object as source.
        :param snapshots: a list of snapshot objects in group_snapshot.
        :param source_group: the Group object as source.
        :param source_vols: a list of volume objects in the source_group.
        :returns: model_update, volumes_model_update
        """
        cgr_type = None
        if volume_utils.is_group_a_cg_snapshot_type(group):
            if volume_utils.is_group_a_type(
                    group, "consistent_group_replication_enabled"):
                cgr_type = True
            return self.create_consistencygroup_from_src(ctxt,
                                                         group,
                                                         volumes,
                                                         group_snapshot,
                                                         snapshots,
                                                         source_group,
                                                         source_vols,
                                                         cgr_type)

        # If it wasn't a consistency group request ignore it and we'll rely on
        # the generic group implementation.
        raise NotImplementedError()

    def create_group_snapshot(self, ctxt, group_snapshot, snapshots):
        """Creates a group_snapshot.

        :param ctxt: the context of the caller.
        :param group_snapshot: the GroupSnapshot object to be created.
        :param snapshots: a list of Snapshot objects in the group_snapshot.
        :returns: model_update, snapshots_model_update
        """
        if volume_utils.is_group_a_cg_snapshot_type(group_snapshot):
            return self.create_cgsnapshot(ctxt, group_snapshot, snapshots)

        # If it wasn't a consistency group request ignore it and we'll rely on
        # the generic group implementation.
        raise NotImplementedError()

    def delete_group_snapshot(self, ctxt, group_snapshot, snapshots):
        """Deletes a group_snapshot.

        :param ctxt: the context of the caller.
        :param group_snapshot: the GroupSnapshot object to be deleted.
        :param snapshots: a list of snapshot objects in the group_snapshot.
        :returns: model_update, snapshots_model_update
        """
        if volume_utils.is_group_a_cg_snapshot_type(group_snapshot):
            return self.delete_cgsnapshot(ctxt, group_snapshot, snapshots)

        # If it wasn't a consistency group request ignore it and we'll rely on
        # the generic group implementation.
        raise NotImplementedError()

    @staticmethod
    def _get_replication_type_from_vol_type(volume_type):
        if volume_type and volume_type.is_replicated():
            specs = volume_type.get("extra_specs")
            if specs and EXTRA_SPECS_REPL_TYPE in specs:
                replication_type_spec = specs[EXTRA_SPECS_REPL_TYPE]
                # Do not validate settings, ignore invalid.
                if replication_type_spec == "<in> async":
                    return REPLICATION_TYPE_ASYNC
                elif replication_type_spec == "<in> sync":
                    return REPLICATION_TYPE_SYNC
                elif replication_type_spec == "<in> trisync":
                    return REPLICATION_TYPE_TRISYNC
            else:
                # if no type was specified but replication is enabled assume
                # that async replication is enabled
                return REPLICATION_TYPE_ASYNC
        return None

    def _group_potential_repl_types(self, pgroup):
        repl_types = set()
        for type in pgroup.volume_types:
            repl_type = self._get_replication_type_from_vol_type(type)
            repl_types.add(repl_type)
        return repl_types

    def _get_pgroup_name(self, pgroup):
        # check if the pgroup has any volume types that are sync rep enabled,
        # if so, we need to use a group name accounting for the ActiveCluster
        # pod.
        base_name = ""
        if ((REPLICATION_TYPE_SYNC in
                self._group_potential_repl_types(pgroup)) or
                (REPLICATION_TYPE_TRISYNC in
                    self._group_potential_repl_types(pgroup))):
            base_name = self._replication_pod_name + "::"

        return "%(base)sconsisgroup-%(id)s-cinder" % {
            'base': base_name, 'id': pgroup.id}


    def _get_pgroup_snap_name(self, group_snapshot):
        """Return the name of the pgroup snapshot that Purity will use"""
        return "%s.%s" % (self._get_pgroup_name(group_snapshot.group),
                          self._get_pgroup_snap_suffix(group_snapshot))

    def _get_pgroup_snap_name_from_snapshot(self, snapshot):
        """Return the name of the snapshot that Purity will use."""

        group_snap = None
        if snapshot.group_snapshot:
            group_snap = snapshot.group_snapshot
        elif snapshot.cgsnapshot:
            group_snap = snapshot.cgsnapshot
        volume_name = self._get_vol_name(snapshot.volume)
        if "::" in volume_name:
            volume_name = volume_name.split("::")[1]
        pg_vol_snap_name = "%(group_snap)s.%(volume_name)s" % {
            'group_snap': self._get_pgroup_snap_name(group_snap),
            'volume_name': volume_name
        }
        return pg_vol_snap_name

    @pure_driver_debug_trace
    def retype(self, context, volume, new_type, diff, host):
        """Retype from one volume type to another on the same backend.

        For a Pure Array there is currently no differentiation between types
        of volumes other than some being part of a protection group to be
        replicated for async, or part of a pod for sync replication.
        """

        qos = None
        # TODO: Can remove this once new_type is a VolumeType OVO
        new_type = volume_type.VolumeType.get_by_name_or_id(context,
                                                            new_type['id'])
        previous_vol_replicated = volume.is_replicated()
        new_vol_replicated = (new_type and new_type.is_replicated())

        prev_repl_type = None
        new_repl_type = None

        # See if the type specifies the replication type. If we know it is
        # replicated but doesn't specify a type assume that it is async rep
        # for backwards compatibility. This applies to both old and new types

        if previous_vol_replicated:
            prev_repl_type = self._get_replication_type_from_vol_type(
                volume.volume_type)

        if new_vol_replicated:
            new_repl_type = self._get_replication_type_from_vol_type(new_type)
            if new_repl_type is None:
                new_repl_type = REPLICATION_TYPE_ASYNC

        # There are a few cases we care about, going from non-replicated to
        # replicated, from replicated to non-replicated, and switching
        # replication types.
        model_update = None
        if previous_vol_replicated and not new_vol_replicated:
            if prev_repl_type == REPLICATION_TYPE_ASYNC:
                # Remove from protection group.
                self._disable_async_replication(volume)
                model_update = {
                    "replication_status": fields.ReplicationStatus.DISABLED
                }
            elif prev_repl_type in [REPLICATION_TYPE_SYNC,
                                    REPLICATION_TYPE_TRISYNC]:
                # We can't pull a volume out of a stretched pod, indicate
                # to the volume manager that we need to use a migration instead
                return False, None
        elif not previous_vol_replicated and new_vol_replicated:
            if new_repl_type == REPLICATION_TYPE_ASYNC:
                # Add to protection group.
                self._enable_async_replication(self._get_current_array(),
                                               volume)
                model_update = {
                    "replication_status": fields.ReplicationStatus.ENABLED
                }
            elif new_repl_type in [REPLICATION_TYPE_SYNC,
                                   REPLICATION_TYPE_TRISYNC]:
                # We can't add a volume to a stretched pod, they must be
                # created in one, indicate to the volume manager that it
                # should do a migration.
                return False, None
        elif previous_vol_replicated and new_vol_replicated:
            if prev_repl_type == REPLICATION_TYPE_ASYNC:
                if new_repl_type in [REPLICATION_TYPE_SYNC,
                                     REPLICATION_TYPE_TRISYNC]:
                    # We can't add a volume to a stretched pod, they must be
                    # created in one, indicate to the volume manager that it
                    # should do a migration.
                    return False, None
            if prev_repl_type == REPLICATION_TYPE_SYNC:
                if new_repl_type == REPLICATION_TYPE_ASYNC:
                    # We can't move a volume in or out of a pod, indicate to
                    # the manager that it should do a migration for this retype
                    return False, None
                elif new_repl_type == REPLICATION_TYPE_TRISYNC:
                    # Add to trisync protection group
                    self._enable_trisync_replication(self._get_current_array(),
                                                     volume)
            if prev_repl_type == REPLICATION_TYPE_TRISYNC:
                if new_repl_type == REPLICATION_TYPE_ASYNC:
                    # We can't move a volume in or out of a pod, indicate to
                    # the manager that it should do a migration for this retype
                    return False, None
                elif new_repl_type == REPLICATION_TYPE_SYNC:
                    # Remove from trisync protection group
                    self._disable_trisync_replication(
                        self._get_current_array(), volume
                    )

        # If we are moving to a volume type with QoS settings then
        # make sure the volume gets the correct new QoS settings.
        # This could mean removing existing QoS settings.
        current_array = self._get_current_array()
        qos = self._get_qos_settings(new_type)
        vol_name = self._generate_purity_vol_name(volume)
        if qos is not None:
            self.set_qos(current_array, vol_name, qos)
        else:
            current_array.patch_volumes(names=[vol_name],
                                        volume=flasharray.VolumePatch(
                                            qos=flasharray.Qos(
                                                iops_limit=100000000,
                                                bandwidth_limit=549755813888)))

        return True, model_update
    
    @pure_driver_debug_trace
    def _disable_async_replication(self, volume):
        """Disable replication on the given volume."""

        current_array = self._get_current_array()
        LOG.debug("Disabling replication for volume %(id)s residing on "
                  "array %(backend_id)s.",
                  {"id": volume["id"],
                   "backend_id": current_array.backend_id})
        res = current_array.delete_protection_groups_volumes(
            group_names=[self._replication_pg_name],
            member_names=[self._get_vol_name(volume)])
        if res.status_code == 400:
            with excutils.save_and_reraise_exception() as ctxt:
                if ERR_MSG_COULD_NOT_BE_FOUND in res.errors[0].message:
                    ctxt.reraise = False
                    LOG.warning("Disable replication on volume failed: "
                                "already disabled: %s",
                                res.errors[0].message)
                else:
                    LOG.error("Disable replication on volume failed with "
                              "message: %s",
                              res.errors[0].message)

    @pure_driver_debug_trace
    def failover_host(self, context, volumes, secondary_id=None, groups=None):
        """Failover to replication target.

        This function combines calls to failover() and failover_completed() to
        perform failover when Active/Active is not enabled.
        """
        active_backend_id, volume_update_list, group_update_list = (
            self.failover(context, volumes, secondary_id, groups))
        self.failover_completed(context, active_backend_id)
        return active_backend_id, volume_update_list, group_update_list

    @pure_driver_debug_trace
    def failover_completed(self, context, active_backend_id=None):
        """Failover to replication target."""
        LOG.info('Driver failover completion started.')
        current = self._get_current_array()
        # This should not happen unless we receive the same RPC message twice
        if active_backend_id == current.backend_id:
            LOG.info('No need to switch replication backend, already using it')
        # Manager sets the active_backend to '' when secondary_id was default,
        # but the driver failover_host method calls us with "default"
        elif not active_backend_id or active_backend_id == 'default':
            LOG.info('Failing back to %s', self._failed_over_primary_array)
            self._swap_replication_state(current,
                                         self._failed_over_primary_array,
                                         failback=True)
        else:
            secondary = self._get_secondary(active_backend_id)
            LOG.info('Failing over to %s', secondary.backend_id)
            self._swap_replication_state(current,
                                         secondary)
        LOG.info('Driver failover completion completed.')

    @pure_driver_debug_trace
    def failover(self, context, volumes, secondary_id=None, groups=None):
        """Failover backend to a secondary array

        This action will not affect the original volumes in any
        way and it will stay as is. If a subsequent failover is performed we
        will simply overwrite the original (now unmanaged) volumes.
        """
        if secondary_id == 'default':
            # We are going back to the 'original' driver config, just put
            # our current array back to the primary.
            if self._failed_over_primary_array:

                # If the "default" and current host are in an ActiveCluster
                # with volumes stretched between the two then we can put
                # the sync rep enabled volumes into available states, anything
                # else will go into an error state pending an admin to check
                # them and adjust states as appropriate.

                current_array = self._get_current_array(True)
                repl_type = current_array.replication_type
                is_in_ac = bool(repl_type == REPLICATION_TYPE_SYNC)
                model_updates = []

                # We are only given replicated volumes, but any non sync rep
                # volumes should go into error upon doing a failback as the
                # async replication is not bi-directional.
                for vol in volumes:
                    repl_type = self._get_replication_type_from_vol_type(
                        vol.volume_type)
                    if not (is_in_ac and repl_type == REPLICATION_TYPE_SYNC):
                        model_updates.append({
                            'volume_id': vol['id'],
                            'updates': {
                                'status': 'error',
                            }
                        })
                return secondary_id, model_updates, []
            else:
                msg = _('Unable to failback to "default", this can only be '
                        'done after a failover has completed.')
                raise exception.InvalidReplicationTarget(message=msg)

        current_array = self._get_current_array(True)
        LOG.debug("Failover replication for array %(primary)s to "
                  "%(secondary)s.",
                  {"primary": current_array.backend_id,
                   "secondary": secondary_id})

        if secondary_id == current_array.backend_id:
            raise exception.InvalidReplicationTarget(
                reason=_("Secondary id can not be the same as primary array, "
                         "backend_id = %(secondary)s.") %
                {"secondary": secondary_id}
            )

        secondary_array = None
        pg_snap = None  # used for async only
        if secondary_id:
            secondary_array = self._get_secondary(secondary_id)
            if secondary_array.replication_type in [REPLICATION_TYPE_ASYNC,
                                                    REPLICATION_TYPE_SYNC]:
                pg_snap = self._get_latest_replicated_pg_snap(
                    secondary_array,
                    self._get_current_array().array_name,
                    self._replication_pg_name
                )
        else:
            LOG.debug('No secondary array id specified, checking all targets.')
            # Favor sync-rep targets options
            secondary_array = self._find_sync_failover_target()

            if not secondary_array:
                # Now look for an async one
                secondary_array, pg_snap = self._find_async_failover_target()

        # If we *still* don't have a secondary array it means we couldn't
        # determine one to use. Stop now.
        if not secondary_array:
            raise PureDriverException(
                reason=_("Unable to find viable secondary array from "
                         "configured targets: %(targets)s.") %
                {"targets": str(self._replication_target_arrays)}
            )

        LOG.debug("Starting failover from %(primary)s to %(secondary)s",
                  {"primary": current_array.array_name,
                   "secondary": secondary_array.array_name})

        model_updates = []
        if secondary_array.replication_type == REPLICATION_TYPE_ASYNC:
            model_updates = self._async_failover_host(
                volumes, secondary_array, pg_snap)
        elif secondary_array.replication_type == REPLICATION_TYPE_SYNC:
            model_updates = self._sync_failover_host(volumes, secondary_array)

        current_array = self._get_current_array(True)

        return secondary_array.backend_id, model_updates, []

    def _swap_replication_state(self, current_array, secondary_array,
                                failback=False):
        # After failover we want our current array to be swapped for the
        # secondary array we just failed over to.
        self._failed_over_primary_array = current_array

        # Remove the new primary from our secondary targets
        if secondary_array in self._replication_target_arrays:
            self._replication_target_arrays.remove(secondary_array)

        # For async, if we're doing a failback then add the old primary back
        # into the replication list
        if failback:
            self._replication_target_arrays.append(current_array)
            self._is_replication_enabled = True
            self._failed_over_primary_array = None

        # If its sync rep then swap the two in their lists since it is a
        # bi-directional setup, if the primary is still OK or comes back
        # it can continue being used as a secondary target until a 'failback'
        # occurs. This is primarily important for "uniform" environments with
        # attachments to both arrays. We may need to adjust flags on the
        # primary array object to lock it into one type of replication.
        if secondary_array.replication_type == REPLICATION_TYPE_SYNC:
            self._is_active_cluster_enabled = True
            self._is_replication_enabled = True
            if secondary_array in self._active_cluster_target_arrays:
                self._active_cluster_target_arrays.remove(secondary_array)

            current_array.replication_type = REPLICATION_TYPE_SYNC
            self._replication_target_arrays.append(current_array)
            self._active_cluster_target_arrays.append(current_array)
        else:
            # If the target is not configured for sync rep it means it isn't
            # part of the ActiveCluster and we need to reflect this in our
            # capabilities.
            self._is_active_cluster_enabled = False
            self._is_replication_enabled = False

        if secondary_array.uniform:
            if secondary_array in self._uniform_active_cluster_target_arrays:
                self._uniform_active_cluster_target_arrays.remove(
                    secondary_array)
            current_array.uniform = True
            self._uniform_active_cluster_target_arrays.append(current_array)

        self._set_current_array(secondary_array)

    def _does_pgroup_exist(self, array, pgroup_name):
        """Return True/False"""
        pgroupres = array.get_protection_groups(
            names=[pgroup_name])
        if pgroupres.status_code == 200:
            return True
        else:
            with excutils.save_and_reraise_exception() as ctxt:
                if ERR_MSG_NOT_EXIST in pgroupres.errors[0].message:
                    ctxt.reraise = False
                    return False
            # Any unexpected exception to be handled by caller.

    @pure_driver_debug_trace
    @utils.retry(PureDriverException,
                 REPL_SETTINGS_PROPAGATE_RETRY_INTERVAL,
                 REPL_SETTINGS_PROPAGATE_MAX_RETRIES)
    def _wait_until_target_group_setting_propagates(
            self, target_array, pgroup_name_on_target):
        # Wait for pgroup to show up on target array.
        if self._does_pgroup_exist(target_array, pgroup_name_on_target):
            return
        else:
            raise PureDriverException(message=_('Protection Group not ready.'))

    @pure_driver_debug_trace
    @utils.retry(PureDriverException,
                 REPL_SETTINGS_PROPAGATE_RETRY_INTERVAL,
                 REPL_SETTINGS_PROPAGATE_MAX_RETRIES)
    def _wait_until_source_array_allowed(self, source_array, pgroup_name):
        result = list(source_array.get_protection_groups_targets(
            group_names=[pgroup_name]).items)[0]
        if result.allowed:
            return
        else:
            raise PureDriverException(message=_('Replication not '
                                                'allowed yet.'))

    def _get_pgroup_name_on_target(self, source_array_name, pgroup_name):
        return "%s:%s" % (source_array_name, pgroup_name)

    @pure_driver_debug_trace
    def _setup_replicated_pods(self, primary, ac_secondaries, pod_name):
        # Make sure the pod exists
        self._create_pod_if_not_exist(primary, pod_name)

        # Stretch it across arrays we have configured, assume all secondary
        # arrays given to this method are configured for sync rep with active
        # cluster enabled.
        for target_array in ac_secondaries:
            res = primary.post_pods_arrays(
                group_names=[pod_name],
                member_names=[target_array.array_name])
            if res.status_code == 400:
                with excutils.save_and_reraise_exception() as ctxt:
                    if (
                        ERR_MSG_ALREADY_EXISTS in res.errors[0].message
                        or ERR_MSG_ARRAY_LIMIT in res.errors[0].message
                    ):
                        ctxt.reraise = False
                        LOG.info("Skipping add array %(target_array)s to pod"
                                 " %(pod_name)s since it's already added.",
                                 {"target_array": target_array.array_name,
                                  "pod_name": pod_name})

    @pure_driver_debug_trace
    def _setup_replicated_pgroups(self, primary, secondaries, pg_name,
                                  replication_interval, retention_policy):
        self._create_protection_group_if_not_exist(
            primary, pg_name)

        # Apply retention policies to a protection group.
        # These retention policies will be applied on the replicated
        # snapshots on the target array.
        primary.patch_protection_groups(
            names=[pg_name],
            protection_group=flasharray.ProtectionGroup(
                target_retention=retention_policy))

        # Configure replication propagation frequency on a
        # protection group.
        primary.patch_protection_groups(
            names=[pg_name],
            protection_group=flasharray.ProtectionGroup(
                replication_schedule=flasharray.ReplicationSchedule(
                    frequency=replication_interval)))
        for target_array in secondaries:
            # Configure PG to replicate to target_array.
            res = primary.post_protection_groups_targets(
                group_names=[pg_name],
                member_names=[target_array.array_name])
            if res.status_code == 400:
                with excutils.save_and_reraise_exception() as ctxt:
                    if ERR_MSG_ALREADY_INCLUDES in res.errors[0].message:
                        ctxt.reraise = False
                        LOG.info("Skipping add target %(target_array)s"
                                 " to protection group %(pgname)s"
                                 " since it's already added.",
                                 {"target_array": target_array.array_name,
                                  "pgname": pg_name})

        # Wait until "Target Group" setting propagates to target_array.
        pgroup_name_on_target = self._get_pgroup_name_on_target(
            primary.array_name, pg_name)

        if self._is_trisync_enabled:
            pgroup_name_on_target = pg_name.replace("::", ":")

        for target_array in secondaries:
            self._wait_until_target_group_setting_propagates(
                target_array,
                pgroup_name_on_target)
            # Configure the target_array to allow replication from the
            # PG on source_array.
            res = target_array.patch_protection_groups_targets(
                group_names=[pgroup_name_on_target],
                target=flasharray.TargetProtectionGroupPostPatch(
                    allowed=True))
            if res.status_code == 400:
                with excutils.save_and_reraise_exception() as ctxt:
                    if ERR_MSG_ALREADY_ALLOWED in res.errors[0].message:
                        ctxt.reraise = False
                        LOG.info("Skipping allow pgroup %(pgname)s on "
                                 "target array %(target_array)s since "
                                 "it is already allowed.",
                                 {"pgname": pg_name,
                                  "target_array": target_array.array_name})

        # Wait until source array acknowledges previous operation.
        self._wait_until_source_array_allowed(primary, pg_name)
        # Start replication on the PG.
        primary.patch_protection_groups(
            names=[pg_name],
            protection_group=flasharray.ProtectionGroup(
                replication_schedule=flasharray.ReplicationSchedule(
                    enabled=True)))


    @pure_driver_debug_trace
    def _generate_replication_retention(self):
        """Generates replication retention settings in Purity compatible format

        An example of the settings:
        target_all_for = 14400 (i.e. 4 hours)
        target_per_day = 6
        target_days = 4
        The settings above configure the target array to retain 4 hours of
        the most recent snapshots.
        After the most recent 4 hours, the target will choose 4 snapshots
        per day from the previous 6 days for retention

        :return: a dictionary representing replication retention settings
        """
        replication_retention = flasharray.RetentionPolicy(
            all_for_sec=self._replication_retention_short_term,
            per_day=self._replication_retention_long_term_per_day,
            days=self._replication_retention_long_term
        )
        return replication_retention

    @pure_driver_debug_trace
    def _get_latest_replicated_pg_snap(self,
                                       target_array,
                                       source_array_name,
                                       pgroup_name):
        # Get all protection group snapshots where replication has completed.
        # Sort into reverse order to get the latest.
        snap_name = "%s:%s" % (source_array_name, pgroup_name)
        LOG.debug("Looking for snap %(snap)s on array id %(array_id)s",
                  {"snap": snap_name, "array_id": target_array.array_id})
        pg_snaps = list(
            target_array.get_protection_group_snapshots_transfer(
                names=[snap_name],
                destroyed=False,
                filter='progress="1.0"',
                sort=["started-"]).items)
        pg_snap = pg_snaps[0] if pg_snaps else None

        LOG.debug("Selecting snapshot %(pg_snap)s for failover.",
                  {"pg_snap": pg_snap})

        return pg_snap

    @pure_driver_debug_trace
    def _create_pod_if_not_exist(self, source_array, name):
        if not name:
            raise PureDriverException(
                reason=_("Empty string passed for Pod name."))
        res = source_array.post_pods(names=[name], pod=flasharray.PodPost())
        if res.status_code == 400:
            with excutils.save_and_reraise_exception() as ctxt:
                if ERR_MSG_ALREADY_EXISTS in res.errors[0].message:
                    # Happens if the pod already exists
                    ctxt.reraise = False
                    LOG.warning("Skipping creation of pod %s since it "
                                "already exists.", name)
                    return
                if list(source_array.get_pods(
                        names=[name]).items)[0].destroyed:
                    ctxt.reraise = False
                    LOG.warning("Pod %s is deleted but not"
                                " eradicated - will recreate.", name)
                    source_array.delete_pods(names=[name])
                    self._create_pod_if_not_exist(source_array, name)

    @pure_driver_debug_trace
    def _create_protection_group_if_not_exist(self, source_array, pgname):
        if not pgname:
            raise PureDriverException(
                reason=_("Empty string passed for PG name."))
        res = source_array.post_protection_groups(names=[pgname])
        if res.status_code == 400:
            with excutils.save_and_reraise_exception() as ctxt:
                if ERR_MSG_ALREADY_EXISTS in res.errors[0].message:
                    # Happens if the PG already exists
                    ctxt.reraise = False
                    LOG.warning("Skipping creation of PG %s since it "
                                "already exists.", pgname)
                    # We assume PG has already been setup with correct
                    # replication settings.
                    return
                if list(source_array.get_protection_groups(
                        names=[pgname]).items)[0].destroyed:
                    ctxt.reraise = False
                    LOG.warning("Protection group %s is deleted but not"
                                " eradicated - will recreate.", pgname)
                    source_array.delete_protection_groups(names=[pgname])
                    self._create_protection_group_if_not_exist(source_array,
                                                               pgname)

    def _find_async_failover_target(self):
        if not self._replication_target_arrays:
            raise PureDriverException(
                reason=_("Unable to find failover target, no "
                         "secondary targets configured."))
        secondary_array = None
        pg_snap = None
        for array in self._replication_target_arrays:
            if array.replication_type != REPLICATION_TYPE_ASYNC:
                continue
            try:
                secondary_array = array
                pg_snap = self._get_latest_replicated_pg_snap(
                    secondary_array,
                    self._get_current_array().array_name,
                    self._replication_pg_name
                )
                if pg_snap:
                    break
            except Exception:
                LOG.exception('Error finding replicated pg snapshot '
                              'on %(secondary)s.',
                              {'secondary': array.backend_id})
                secondary_array = None

        if not pg_snap:
            raise PureDriverException(
                reason=_("Unable to find viable pg snapshot to use for "
                         "failover on selected secondary array: %(id)s.") %
                {"id": secondary_array.backend_id if secondary_array else None}
            )

        return secondary_array, pg_snap

    def _get_secondary(self, secondary_id):
        for array in self._replication_target_arrays:
            if array.backend_id == secondary_id:
                return array
        raise exception.InvalidReplicationTarget(
            reason=_("Unable to determine secondary_array from"
                     " supplied secondary: %(secondary)s.") %
            {"secondary": secondary_id}
        )

    def _find_sync_failover_target(self):
        secondary_array = None
        if not self._active_cluster_target_arrays:
            LOG.warning("Unable to find failover target, no "
                        "sync rep secondary targets configured.")
            return secondary_array

        for array in self._active_cluster_target_arrays:
            secondary_array = array
            # Ensure the pod is in a good state on the array
            res = secondary_array.get_pods(
                names=[self._replication_pod_name])
            if res.status_code == 200:
                pod_info = list(res.items)[0]
                for pod_array in range(0, len(pod_info.arrays)):
                    # Compare against Purity ID's
                    if pod_info.arrays[pod_array].id == \
                            secondary_array.array_id:
                        if pod_info.arrays[pod_array].status == "online":
                            # Success! Use this array.
                            break
                        else:
                            secondary_array = None
            else:
                LOG.warning("Failed to get pod status for secondary array "
                            "%(id)s: %(err)s",
                            {
                                "id": secondary_array.backend_id,
                                "err": res.errors[0].message,
                            })
                secondary_array = None
        return secondary_array

    def _async_failover_host(self, volumes, secondary_array, pg_snap):
        # Try to copy the flasharray as close as we can.
        secondary_info = list(secondary_array.get_arrays().items)[0]
        if version.parse(secondary_info.version) < version.parse('6.3.4'):
            secondary_safemode = False
        else:
            secondary_safemode = True

        volume_snaps = list(secondary_array.get_volume_snapshots(
            filter="name='" + pg_snap.name + ".*'"
        ).items)

        # We only care about volumes that are in the list we are given.
        vol_names = set()
        for vol in volumes:
            vol_names.add(self._get_vol_name(vol))

        for snap in range(0, len(volume_snaps)):
            vol_name = volume_snaps[snap].name.split('.')[-1]
            if vol_name in vol_names:
                vol_names.remove(vol_name)
                LOG.debug('Creating volume %(vol)s from replicated snapshot '
                          '%(snap)s', {'vol': vol_name,
                                       'snap': volume_snaps[snap].name})
                if secondary_safemode:
                    secondary_array.post_volumes(
                        with_default_protection=False,
                        volume=flasharray.VolumePost(
                            source=flasharray.Reference(
                                name=volume_snaps[snap].name)
                        ),
                        names=[vol_name],
                        overwrite=True)
                else:
                    secondary_array.post_volumes(
                        volume=flasharray.VolumePost(
                            source=flasharray.Reference(
                                name=volume_snaps[snap].name)
                        ),
                        names=[vol_name],
                        overwrite=True)
            else:
                LOG.debug('Ignoring unmanaged volume %(vol)s from replicated '
                          'snapshot %(snap)s.', {'vol': vol_name,
                                                 'snap': snap['name']})
        # The only volumes remaining in the vol_names set have been left behind
        # on the array and should be considered as being in an error state.
        model_updates = []
        for vol in volumes:
            if self._get_vol_name(vol) in vol_names:
                model_updates.append({
                    'volume_id': vol['id'],
                    'updates': {
                        'status': 'error',
                    }
                })
            else:
                repl_status = fields.ReplicationStatus.FAILED_OVER
                model_updates.append({
                    'volume_id': vol['id'],
                    'updates': {
                        'replication_status': repl_status,
                    }
                })
        return model_updates

    def _sync_failover_host(self, volumes, secondary_array):
        """Perform a failover for hosts in an ActiveCluster setup

        There isn't actually anything that needs to be changed, only
        update the volume status to distinguish the survivors..
        """

        array_volumes = list(secondary_array.get_volumes(
            filter="pod.name='" + self._replication_pod_name + "'").items)
        replicated_vol_names = set()
        for vol in array_volumes:
            replicated_vol_names.add(vol.name)

        model_updates = []
        for vol in volumes:
            if self._get_vol_name(vol) not in replicated_vol_names:
                model_updates.append({
                    'volume_id': vol['id'],
                    'updates': {
                        'status': fields.VolumeStatus.ERROR,
                    }
                })
            else:
                repl_status = fields.ReplicationStatus.FAILED_OVER
                model_updates.append({
                    'volume_id': vol['id'],
                    'updates': {
                        'replication_status': repl_status,
                    }
                })
        return model_updates
