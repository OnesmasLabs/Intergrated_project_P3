from setuptools import setup, find_packages
setup(
    name='ValidateData',
    version='0.1',
    packages=find_packages(exclude=['test*']),
    license='MIT',
    description='EDSA example python package',
    long_description=open('README.md').read(),
    install_requires=['numpy'],
    url='https://github.com/OnesmasLabs/mypackage.git',
    author='OnesmasLabs,',
    author_email='terrybetty97@students.tukenya.ac.ke'
)