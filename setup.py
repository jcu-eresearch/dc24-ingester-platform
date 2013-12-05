from setuptools import setup

requires = [
    "pysandbox",
    "Twisted",
    "sqlalchemy",
    "ipython",
    "jcu.dc24.ingesterapi",
    "lxml",
    "requests",
    "pysqlite",
    "simplesos"
    ]

setup(name='jcu.dc24.ingesterplatform',
      version='0.0.1dev',
      description='DC24 Ingester Platform',
      author='Nigel Sim',
      author_email='nigel.sim@coastalcoms.com',
      url='http://www.coastalcoms.com',
      packages=['dc24_ingester_platform'],
      zip_safe=False,
      install_requires=requires,
      entry_points={
          "console_scripts": [
          "run_ingester = dc24_ingester_platform.ingester.data_sources:main_ingress",
          "run_script = dc24_ingester_platform.ingester.data_sources:main_script"]
      },
)
#      package_data={'twisted.plugins': ['twisted/plugins/dc24_ingester_platform.py']},
