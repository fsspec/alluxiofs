# Alluxioio package

The Alluxioio package replaces Python's POSIX-related standard methods with custom methods that use Alluxiofs. By importing the package, users don't need to modify their existing code to use AlluxioFS. Python's POSIX-related methods will be automatically replaced by Alluxiofs methods. This replacement is only effective in the file that imports the Alluxioio package, as well as in external libraries that are imported after Alluxioio.
Alluxio customized methods will be used only when the file or directory paths starts with supported protocols such as "s3://" and "hdfs://". Paths that don't start with supported protocols will still use Python's original methods.
### Customized Python Methods

- builtins.open
- os.listdir
- os.mkdir
- os.rmdir
- os.rename
- os.remove
- shutil.copy
- os.stat
- os.path.isdir
- os.path.isfile
- os.path.exists
- os.walk

### How To Use
Add a configuration yaml file with name "alluxiofs_config.yaml" in the same directory of files that you want to use alluxioio.
The configurations are used to initialize alluxiofs filesystem.
```
etcd_hosts: "localhost"
etcd_port: 2379
options:
  alluxio.user.file.replication.max: "3"
concurrency: 128
worker_http_port: 8080
preload_path: "/path/to/preload"
target_protocol: "s3"
target_options:
  aws_access_key_id: "your-access-key-id"
  aws_secret_access_key: "your-secret-access-key"

```

Import alluxioio as the first line of import in your code:
```
from alluxiofs import alluxioio
import os

with open('s3://file_name.csv', 'r') as f:
    print(f.read())

for d in os.listdir('s3://folder_name'):
    print(d)
```
