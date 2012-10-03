from setuptools import setup
 
setup(name='DC24 Ingester Platform',
      version='0.0.1',
      description='DC24 Ingester Platform',
      author='Nigel Sim',
      author_email='nigel.sim@coastalcoms.com',
      url='http://www.coastalcoms.com',
      packages=['dc24_ingester_platform'],
      zip_safe=False,
      entry_points={
          "console_scripts": ["mgmt_client = dc24_ingester_platform.client:main"]
      }
)
#      package_data={'twisted.plugins': ['twisted/plugins/dc24_ingester_platform.py']},
