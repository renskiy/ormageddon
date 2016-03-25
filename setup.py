from distutils.core import setup

with open('README.md') as description:
    long_description = description.read()

setup(
    name='ormageddon',
    version='0.1',
    author='Rinat Khabibiev',
    author_email='srenskiy@gmail.com',
    packages=[
        'ormageddon',
        'ormageddon/db',
    ],
    url='https://github.com/renskiy/ormageddon',
    license='MIT',
    description='ORMageddon',
    long_description=long_description,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.5',
    ],
    install_requires=[
        'aiopg==0.9.2',
        'cached-property==1.3.0',
        'peewee==2.8.0',
        'tasklocals==0.2',
    ],
)
