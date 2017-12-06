| Configuration option = Default value | Description |
| :--- | :--- |
| **[DEFAULT]** | |
| listener_port_number=8000 | (Port number)Port number plugin uses to listen for communication from the docker engine.|
| mount_path=/docker_volumes/ | (String)Full mount path on host for VMAX volumes.|
| default_volume_size=1 | (Integer)Default volume size to use in creating volume if none is provided.|
| enabled_backends=None | (List)(Required)A list of backend names to use. These backend names should be backed by a unique [CONFIG] group with its options.|
| default_backend=None | (String)Default backend to use. This backend must be included in enabled backends. If not set, the first backend in the enabled_backends list is used volume if none is provided.|
| debug=false | (Boolean)If set to true, the logging level will be set to DEBUG instead of the default INFO level.|
| log_file=None | (String)Name of log file to send logging output to. If no default is set, logging will go to stderr as defined by use_stderr.|
| log_dir=None | (String)The base directory used for relative log_file paths.|
| use_journal=false | (String)Enable journald for logging. If running in a systemd environment you may wish to enable journal support. Doing so will use the journal native protocol which includes structured metadata in addition to log messages.|
| mount_path=/docker_volumes/ | (String)Full mount path on host for VMAX volumes.|

Description of backend configuration options. Each backend configuration 
listed in enabled_backends should have its own [CONFIG] group with its 
options. The description is shown below

| Configuration option = Default value | Description |
| :--- | :--- |
| **[\<backend-name\>]** | |
| volume_backend_name=None | (String)(Required)The backend name for the volume. Should correspond to the name in enabled backends.|
| storage_protocol=iSCSI | (String)Backend protocol to target when creating volume types.|
| port_groups=None | (List)(Required)List of port groups containing frontend ports configured prior for server connection..|
| rest_server_ip=None | (List)(Required)Rest server end point.|
| rest_port_number=8443 | (Port)REST server port number.|
| rest_user_name=None | (String)(Required)Rest server user name.|
| rest_password=None | (String)(Required)Rest server password.|
| array=None | (String)(Required)Serial number of the array to connect to.|
| srp=None | (String)(Required)Storage resource pool on array to use for provisioning.|
| service_level=None | (String)Service level to use for provisioning storage.|
| workload=None | (String)Workload.|
