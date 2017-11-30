==================
docker-volume-vmax
==================


VMAX Docker Volume Plugin


The VMAX Docker Volume Plugin is open source software that provides persistent
block storage for containerized applications using VMAX all Flash Storage.
This plugin is an out-of-process extension that should be run on same host
as the docker daemon.

WHAT'S SUPPORTED
----------------

This package supports Unisphere version 8.4 onwards, although the calls
should work on 8.0, 8.1, 8.2 and 8.3 also. We support VMAX3 and VMAX All-Flash
(All Flash from 8.3 onwards).

INSTALLATION
------------

docker-volume-vmax can be installed and used in 2 ways depending on the implemetation; git install and full install.

GIT INSTALL
===========

To install via git, follow the steps below
::
  git clone https://github.com/okpoyu/docker-volume-vmax.git
  cd docker-volume-vmax
  sudo pip install -r requirements.txt
  cp config/vmax.conf.sample /path/to/vmax.conf
Edit vmax.conf using the storage configuration of the vmax at your disposal. Finally run the command below to
::
  sudo python listener.py /path/to/vmax.conf

FULL INSTALL
============

Full installation requires access to systemd for services. Follow the steps below
::
  sudo pip install docker-volume-vmax
  sudo cp /etc/vmax/vmax.conf.sample /etc/vmax/vmax.conf

In /etc/vmax directory, edit vmax.conf using the storage configuration of the vmax. Make systemd aware of vmaxAF.service
::
  sudo systemctl daemon-reload
Start the vmaxAF service
::
  sudo systemctl start vmaxAF.service

USAGE
-----

The following are the currently supported actions that can be taken using the VMAX Docker volume plugin

Creating a VMAX volume
======================

::

  sudo docker volume create  --driver vmaxAF \
                             --name <vol_name> \
                             -o size=1

There are several optional parameters that can be used during volume creation and are passed in after the -o parameters

- size - specifies the size in GB of the volume to create. Defaults to 1GB if not specified
- backend-name - specifies the backend name to use in volume creation if multiple backends are available. Defaults to the default backend specified in the configuration file. If no default backend is specified in the configuration file, then the first backend listed is used

Mounting a volume
======================

Use the following command to mount a volume and start a bash prompt:

::

  sudo docker run -ti \
                --name <container_name> \
                --volume-driver=vmaxAF \
                -v <vol_name>:/<mount_point> \
                <image_name> /bin/bash

Unmounting a volume
======================

Exiting the bash prompt will cause the volume to unmount:

::

  exit

Stop the container and delete container
======================

::

  sudo docker stop <container_name>
  sudo docker rm <container_name>

Deleting a volume
======================

::

  sudo docker volume rm <vol_name>

Credits
---------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage

