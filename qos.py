from oslo_log import log as logging
from cinder import context
from cinder import exception
from cinder.volume import qos_specs
from base_driver import PureBaseVolumeDriver
from pypureclient import flasharray
from utils import pure_driver_debug_trace

LOG = logging.getLogger(__name__)

class PureQoSMixin(PureBaseVolumeDriver):
    """Handles Quality of Service (QoS) settings for volumes."""

    @pure_driver_debug_trace
    def set_qos(self, array, vol_name, qos):
        """Apply QoS settings to a volume."""
        if qos['maxIOPS'] == 0 and qos['maxBWS'] == 0:
            array.patch_volumes(names=[vol_name],
                                volume=flasharray.VolumePatch(
                                    qos=flasharray.Qos(
                                        iops_limit=100000000,
                                        bandwidth_limit=549755813888)))
        elif qos['maxIOPS'] == 0:
            array.patch_volumes(names=[vol_name],
                                volume=flasharray.VolumePatch(
                                    qos=flasharray.Qos(
                                        iops_limit=100000000,
                                        bandwidth_limit=qos['maxBWS'])))
        elif qos['maxBWS'] == 0:
            array.patch_volumes(names=[vol_name],
                                volume=flasharray.VolumePatch(
                                    qos=flasharray.Qos(
                                        iops_limit=qos['maxIOPS'],
                                        bandwidth_limit=549755813888)))
        else:
            array.patch_volumes(names=[vol_name],
                                volume=flasharray.VolumePatch(
                                    qos=flasharray.Qos(
                                        iops_limit=qos['maxIOPS'],
                                        bandwidth_limit=qos['maxBWS'])))
        return

    @pure_driver_debug_trace
    def create_with_qos(self, array, vol_name, vol_size, qos):
        """Create a volume with QoS limits."""
        if self._array.safemode:
            if qos['maxIOPS'] == 0 and qos['maxBWS'] == 0:
                array.post_volumes(names=[vol_name],
                                   with_default_protection=False,
                                   volume=flasharray.VolumePost(
                                       provisioned=vol_size))
            elif qos['maxIOPS'] == 0:
                array.post_volumes(names=[vol_name],
                                   with_default_protection=False,
                                   volume=flasharray.VolumePost(
                                       provisioned=vol_size,
                                       qos=flasharray.Qos(
                                           bandwidth_limit=qos['maxBWS'])))

            elif qos['maxBWS'] == 0:
                array.post_volumes(names=[vol_name],
                                   with_default_protection=False,
                                   volume=flasharray.VolumePost(
                                       provisioned=vol_size,
                                       qos=flasharray.Qos(
                                           iops_limit=qos['maxIOPS'])))

            else:
                array.post_volumes(names=[vol_name],
                                   with_default_protection=False,
                                   volume=flasharray.VolumePost(
                                       provisioned=vol_size,
                                       qos=flasharray.Qos(
                                           iops_limit=qos['maxIOPS'],
                                           bandwidth_limit=qos['maxBWS'])))
        else:
            if qos['maxIOPS'] == 0 and qos['maxBWS'] == 0:
                array.post_volumes(names=[vol_name],
                                   volume=flasharray.VolumePost(
                                       provisioned=vol_size))
            elif qos['maxIOPS'] == 0:
                array.post_volumes(names=[vol_name],
                                   volume=flasharray.VolumePost(
                                       provisioned=vol_size,
                                       qos=flasharray.Qos(
                                           bandwidth_limit=qos['maxBWS'])))
            elif qos['maxBWS'] == 0:
                array.post_volumes(names=[vol_name],
                                   volume=flasharray.VolumePost(
                                       provisioned=vol_size,
                                       qos=flasharray.Qos(
                                           iops_limit=qos['maxIOPS'])))
            else:
                array.post_volumes(names=[vol_name],
                                   volume=flasharray.VolumePost(
                                       provisioned=vol_size,
                                       qos=flasharray.Qos(
                                           iops_limit=qos['maxIOPS'],
                                           bandwidth_limit=qos['maxBWS'])))
        return


    def _get_qos_settings(self, volume_type):
        """Get extra_specs and qos_specs of a volume_type.

        This fetches the keys from the volume type. Anything set
        from qos_specs will override keys set from extra_specs
        """

        # Deal with volume with no type
        qos = {}
        qos_specs_id = volume_type.get('qos_specs_id')
        specs = volume_type.get('extra_specs')
        # We prefer QoS specs associations to override
        # any existing extra-specs settings
        if qos_specs_id is not None:
            ctxt = context.get_admin_context()
            kvs = qos_specs.get_qos_specs(ctxt, qos_specs_id)['specs']
        else:
            kvs = specs

        for key, value in kvs.items():
            if key in self.PURE_QOS_KEYS:
                qos[key] = value
        if qos == {}:
            return None
        else:
            # Chack set vslues are within limits
            iops_qos = int(qos.get('maxIOPS', 0))
            bw_qos = int(qos.get('maxBWS', 0)) * 1048576
            if iops_qos != 0 and not (100 <= iops_qos <= 100000000):
                msg = _('maxIOPS QoS error. Must be more than '
                        '100 and less than 100000000')
                raise exception.InvalidQoSSpecs(message=msg)
            if bw_qos != 0 and not (1048576 <= bw_qos <= 549755813888):
                msg = _('maxBWS QoS error. Must be between '
                        '1 and 524288')
                raise exception.InvalidQoSSpecs(message=msg)

            qos['maxIOPS'] = iops_qos
            qos['maxBWS'] = bw_qos
        return qos
    