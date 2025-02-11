# Configuration

from oslo_config import cfg
from cinder.volume import configuration

PURE_OPTS = [
    cfg.StrOpt("pure_api_token",
               help="REST API authorization token."),
    cfg.BoolOpt("pure_automatic_max_oversubscription_ratio",
                default=True,
                help="Automatically determine an oversubscription ratio based "
                     "on the current total data reduction values. If used "
                     "this calculated value will override the "
                     "max_over_subscription_ratio config option."),
    cfg.StrOpt("pure_host_personality",
               default=None,
               choices=['aix', 'esxi', 'hitachi-vsp', 'hpux',
                        'oracle-vm-server', 'solaris', 'vms', None],
               help="Determines how the Purity system tunes the protocol used "
                    "between the array and the initiator."),
    # These are used as default settings.  In future these can be overridden
    # by settings in volume-type.
    cfg.IntOpt("pure_replica_interval_default", default=3600,
               help="Snapshot replication interval in seconds."),
    cfg.IntOpt("pure_replica_retention_short_term_default", default=14400,
               help="Retain all snapshots on target for this "
                    "time (in seconds.)"),
    cfg.IntOpt("pure_replica_retention_long_term_per_day_default", default=3,
               help="Retain how many snapshots for each day."),
    cfg.IntOpt("pure_replica_retention_long_term_default", default=7,
               help="Retain snapshots per day on target for this time "
                    "(in days.)"),
    cfg.StrOpt("pure_replication_pg_name", default="cinder-group",
               help="Pure Protection Group name to use for async replication "
                    "(will be created if it does not exist)."),
    cfg.StrOpt("pure_trisync_pg_name", default="cinder-trisync",
               help="Pure Protection Group name to use for trisync "
                    "replication leg inside the sync replication pod "
                    "(will be created if it does not exist)."),
    cfg.StrOpt("pure_replication_pod_name", default="cinder-pod",
               help="Pure Pod name to use for sync replication "
                    "(will be created if it does not exist)."),
    cfg.StrOpt("pure_iscsi_cidr", default="0.0.0.0/0",
               help="CIDR of FlashArray iSCSI targets hosts are allowed to "
                    "connect to. Default will allow connection to any "
                    "IPv4 address. This parameter now supports IPv6 subnets. "
                    "Ignored when pure_iscsi_cidr_list is set."),
    cfg.ListOpt("pure_iscsi_cidr_list", default=None,
                help="Comma-separated list of CIDR of FlashArray iSCSI "
                     "targets hosts are allowed to connect to. It supports "
                     "IPv4 and IPv6 subnets. This parameter supersedes "
                     "pure_iscsi_cidr."),
    cfg.StrOpt("pure_nvme_cidr", default="0.0.0.0/0",
               help="CIDR of FlashArray NVMe targets hosts are allowed to "
                    "connect to. Default will allow connection to any "
                    "IPv4 address. This parameter now supports IPv6 subnets. "
                    "Ignored when pure_nvme_cidr_list is set."),
    cfg.ListOpt("pure_nvme_cidr_list", default=None,
                help="Comma-separated list of CIDR of FlashArray NVMe "
                     "targets hosts are allowed to connect to. It supports "
                     "IPv4 and IPv6 subnets. This parameter supersedes "
                     "pure_nvme_cidr."),
    cfg.StrOpt("pure_nvme_transport", default="roce",
               choices=['roce', 'tcp'],
               help="The NVMe transport layer to be used by the NVMe driver."),
    cfg.BoolOpt("pure_eradicate_on_delete",
                default=False,
                help="When enabled, all Pure volumes, snapshots, and "
                     "protection groups will be eradicated at the time of "
                     "deletion in Cinder. Data will NOT be recoverable after "
                     "a delete with this set to True! When disabled, volumes "
                     "and snapshots will go into pending eradication state "
                     "and can be recovered."),
    cfg.BoolOpt("pure_trisync_enabled",
                default=False,
                help="When enabled and two replication devices are provided, "
                     "one each of types sync and async, this will enable "
                     "the ability to create a volume that is sync replicated "
                     "to one array and async replicated to a separate array.")
]

CONF = cfg.CONF
CONF.register_opts(PURE_OPTS, group=configuration.SHARED_CONF_GROUP)