# Cascalog-Lzo

Based on the excellent work in Elephant-Bird:

    https://github.com/dvryaboy/elephant-bird/tree/eb-dev

NOTE: If you just want to read .lzo files you just need to setup hadoop to do so. Then the normal `(hfs-textline "my_lzo_file.lzo")` will work. 
AWS EMR is setup to include hadoop-lzo by default.

### Configuring Hadoop

You can find more information about Hadoop-LZO [on Cloudera](http://www.cloudera.com/blog/2009/11/hadoop-at-twitter-part-1-splittable-lzo-compression/).

[Quick install guide](http://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.0.9.0/bk_installing_manually_book/content/rpm-chap2-3.html)

## Usage

Add the following to `project.clj`:

    [cascalog/cascalog-lzo "3.0.0-SNAPSHOT"]

To use: `(:require [cascalog.lzo :as lzo])` and then create sinks or sources as `(lzo/hfs-lzo-textline directory)`

Tested with hadoop 2.4 and 2.6.

Stay tuned for updates!

### Installing Local Dependencies

On OS X:

1. Install MacPorts
2. sudo port install lzo
3. If you're on Lion, you'll have to re-install your java development headers [here](http://connect.apple.com/cgi-bin/WebObjects/MemberSite.woa/wa/download?path=%2FDeveloper_Tools%2Fjava_for_mac_os_x_10.7_update_1_developer_package%2Fjavadeveloper_for_mac_os_x_10.7__11m3527.dmg&wosid=Mo5ndLZsjioK2DIXcKKGLmyLffK).
4. Download the [lzo native libs](https://github.com/nathanmarz/cascalog-contrib/downloads) and place them in `/opt/local/lib`.

### Building Hadoop-Lzo

This is only necessary if you're trying to rebuild this project.

```bash
git clone https://github.com/twitter/hadoop-lzo
cd hadoop-lzo

JAVA_HOME=$(/usr/libexec/java_home) \
C_INCLUDE_PATH=/opt/local/include LIBRARY_PATH=/opt/local/lib \
CFLAGS="-arch x86_64" mvn clean test install
```
