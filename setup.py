#!/usr/bin/env python

from setuptools import setup

setup(name='tap-slack',
      version='0.4.3',
      description='Singer.io tap for extracting data from the Slack API',
      author='RIch Clark',
      url='',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_slack'],
      install_requires=[
          'singer-python==1.2.0',
          'requests==2.12.4',
          'backoff==1.3.2'
      ],
      entry_points='''
          [console_scripts]
          tap-slack=tap_slack:main
      ''',
      packages=['tap_slack'],
      package_data = {
          'tap_slack/schemas': [
            'conversation.json',
            'emoji.json',
            'file.json',
            'im.json',
            'message.json',
            'team.json',
            'user.json',
            'usergroup.json'
          ]
      },
      include_package_data=True,
)
