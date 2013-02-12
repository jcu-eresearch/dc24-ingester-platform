from setuptools import setup
 
setup(name='jcu.dc24.ingesterplatform',
      version='0.0.1dev',
      description='DC24 Ingester Platform',
      author='Nigel Sim',
      author_email='nigel.sim@coastalcoms.com',
      url='http://www.coastalcoms.com',
      packages=['dc24_ingester_platform'],
      zip_safe=False,
      entry_points={
          "console_scripts": ["mgmt_client = dc24_ingester_platform.client:main",
          "run_ingester = dc24_ingester_platform.ingester.data_sources:main"]
      },
      install_requires = ["pysandbox", "jcu.dc24.ingesterapi"]
)
#      package_data={'twisted.plugins': ['twisted/plugins/dc24_ingester_platform.py']},
