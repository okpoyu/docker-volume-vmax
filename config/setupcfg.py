from oslo_config import cfg

host_opts = [
    cfg.PortOpt('listener_port_number',
                default=8000,
                help='Host Port Number to use for docker communication'),
    cfg.StrOpt('mount_path',
                default='/docker_volumes/',
                help='Path to mount for volumes'),
    cfg.IntOpt('default_volume_size',
               default=1,
               help='Default volume size.'),
    cfg.ListOpt('enabled_backends',
                help='A list of backend names to use. These backend names '
                     'should be backed by a unique [CONFIG] group '
                     'with its options'),
    cfg.StrOpt('default_backend',
               help='Default backend to use'),
]

volume_opts = [
    cfg.StrOpt('volume_backend_name',
               required=True,
               help='The backend name for the volume. Should correspond to '
                    'the name in enabled backends'),
    cfg.StrOpt('storage_protocol',
               default='iSCSI',
               help='Backend protocol to target when creating volume types'),
    cfg.ListOpt('port_groups',
                bounds=True,
                required=True,
                help='Port groups'),
    cfg.IPOpt('rest_server_ip',
              required=True,
              default=None,
              help='Rest server end point'),
    cfg.PortOpt('rest_port_number',
                default=8443,
                help='rest server port number'),
    cfg.StrOpt('rest_user_name',
               default=None,
               help='Rest server user name'),
    cfg.StrOpt('rest_password',
               secret=True,
               help='Rest server password'),
    cfg.StrOpt('array',
               required=True,
               help='Array'),
    cfg.StrOpt('srp',
               help='srp'),
    cfg.StrOpt('service_level',
               help='service level'),
    cfg.StrOpt('workload',
               help='workload'),

]
