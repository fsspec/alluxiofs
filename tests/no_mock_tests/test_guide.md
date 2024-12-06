# How to Test

## Test Environment
+ Alluxio Condinator
+ Alluxio Worker

alluio-site.properties (For reference only)
```
#
# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.
#

#
# Common properties
#
# alluxio.license=<Your License String>
# alluxio.master.hostname=localhost

#
# UFS Mount table
#
# alluxio.mount.table.source=STATIC_FILE
# alluxio.mount.table.static.conf.file=${alluxio.conf.dir}/mount_table

#
# Worker properties
#
# alluxio.worker.page.store.dirs=/path/to/cache/dir1,/path/to/cache/dir2
# alluxio.worker.page.store.sizes=100GB,400GB

# Master & scheduler related
alluxio.master.hostname=localhost
alluxio.master.journal.type=NOOP

# Security related (disabled)
alluxio.security.authorization.permission.enabled=false
alluxio.security.authentication.type=NOSASL
alluxio.network.tls.enabled=false

# Etcd related
alluxio.worker.membership.manager.type=ETCD
alluxio.mount.table.source=STATIC_FILE
alluxio.mount.table.static.conf.file=/~/enterprise/conf/mount_table
alluxio.etcd.endpoints=http://localhost:2379

#Worker related
alluxio.worker.page.store.sizes=30GB
alluxio.worker.page.store.page.size=1MB
#alluxio.worker.page.store--
alluxio.worker.network.netty.worker.content.length.max=1GB

alluxio.job.batch.size=2000

#user related
#alluxio.user.file.replication.min=1
#alluxio.user.replica.selection.policy=RANDOM

#dora related
#alluxio.dora.file.segment.read.enabled=true
alluxio.dora.file.segment.size=10MB

#Client related
alluxio.user.file.metadata.sync.interval=-1
alluxio.user.file.writetype.default=CACHE_THROUGH
alluxio.user.metadata.cache.max.size=0
alluxio.dora.client.ufs.fallback.enabled=false
alluxio.user.fuse.sync.close.enabled=true
#alluxio.user.position.write.enabled=true
alluxio.user.position.write.frame.size=1KB

#lock related
alluxio.file.lock.manager.unused.lock.expiration.ttl=500

#Fuse related
alluxio.fuse.jnifuse.libfuse.version=3
alluxio.fuse.debug.enabled=true

#alluxio.dora.worker.metastore.rocksdb.dir
#alluxio.worker.page.store.dirs

alluxio.home=/home/yxd/??/enterprise

#ufs related
alluxio.underfs.s3.region=-
alluxio.underfs.s3.server.side.encryption.enabled=true
alluxio.underfs.s3.disable.dns.buckets=true
alluxio.underfs.xattr.change.enabled=false

# yxd
s3a.accessKeyId=-
s3a.secretKey=-

alluxio.underfs.s3.sdk.version=2

#oss related
fs.oss.accessKeyId=-
fs.oss.accessKeySecret=-
fs.oss.endpoint=-
```

## Tests

change the `target_protocol`，`cluster_name` and so on in the following code snippet.
```python
alluxio_fs = fsspec.filesystem(
    "alluxiofs",
    etcd_hosts="localhost",
    etcd_port=2379,
    # target_options=oss_options,
    target_protocol="s3",
    cluster_name="alluxio",
    page_size="1MB"
)
```

change the `bucket_name` to yours, amd create a folder named `test_folder_name` in the bucket in the following code snippet.
```python
bucket_name = "yxd-fsspec"
test_folder_name = "python_sdk_test"
# the format of home_path: s3://{bucket_name}/{test_folder_name}
home_path = "s3://" + bucket_name + "/" + test_folder_name
```

``` bash
python write_test.py
python read_test.py
python other_option_test.py
```

## Expended Test
To test read and write performance of different size of file, you can create some different size of files in thr path '../assets'. such as 1KB, 1MB, 10MB, 100MB, 1GB, 10GB, ... ,
which named as 'random1KB.txt', 'random1MB.txt', 'random10MB.txt', 'random100MB.txt', 'random1GB.txt', 'random10GB.txt', ... .
