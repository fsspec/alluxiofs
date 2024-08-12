# Alluxioio package

Alluxioio package replaces Python's POSIX related standard methods with
custom methods that uses alluxiofs. By importing the package, users don't
need to modify their existing code to use alluxiofs. Python's POSIX related methods
will be automatically replaced by alluxiofs methods. The replacement is only effective
in the file that imports alluxioio package as well as in external libraries that were imported
after alluxioio.

### Customized Python Methods

builtins.open = alluxio_open
os.listdir = alluxio_ls
os.mkdir = alluxio_mkdir
os.rmdir = alluxio_rmdir
os.rename = alluxio_rename
os.remove = alluxio_remove
shutil.copy = alluxio_copy
os.stat = alluxio_stat
os.path.isdir = alluxio_isdir
os.path.isfile = alluxio_isfile
os.path.exists = alluxio_exists
os.walk = alluxio_walk

### How To Use
Add a configuration yaml file in the same directory of files that you want to use alluxioio.
The configurations are used to initialize alluxiofs filesystem.
Import alluxiofs to use in your code
example
