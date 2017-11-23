from setuptools import setup, find_packages

with open("requirements.txt") as requirements:
    install_requires = requirements.readlines()

setup(name='docker-volume-vmax',
      version='1.0.0',
      description='VMAX Docker Volume Plugin',
      url='https://github.com/okpoyu/docker-volume-vmax/',
      author='Unana Okpoyo',
      author_email='unana.okpoyo@dell.com',
      license='Apache License, Version 2.0',
      packages=find_packages(exclude=['docs', 'tests*']),
      scripts=['bin/inq'],
      install_requires=install_requires,
      zip_safe=False,
      entry_points={
          'console_scripts': [
              'vmaxAF = vmaxafdockerplugin.listener_vmax:main',
          ],
      },
      classifiers=[
          'Development Status :: 3 - Alpha',
          'Intended Audience :: Developers',
          'Intended Audience :: System Administrators',
          'License :: OSI Approved :: Apache Software License',
          'Environment :: Plugins',
          'Programming Language :: Python',
          'Programming Language :: Python :: 2.6',
          'Programming Language :: Python :: 2.7',
          'Programming Language :: Python :: 3.0',
          'Topic :: Internet :: WWW/HTTP',
      ],
      data_files=[
          ('/etc/vmax', ['config/vmax.conf.sample']),
          ('/etc/systemd/system', ['config/vmaxAF.service'])]
      )
