# Cascalog-Lzo

Based on the excellent work in Elephant-Bird:

    https://github.com/dvryaboy/elephant-bird/tree/eb-dev

## Usage

Add the following to `project.clj`:

    [cascalog-lzo "1.10.1-SNAPSHOT"]

Stay tuned for updates!

### Installing Local Dependencies

On OS X:

1. Install MacPorts
2. sudo port install lzo
3. If you're on Lion, you'll have to re-install your java development headers [here](http://connect.apple.com/cgi-bin/WebObjects/MemberSite.woa/wa/download?path=%2FDeveloper_Tools%2Fjava_for_mac_os_x_10.7_update_1_developer_package%2Fjavadeveloper_for_mac_os_x_10.7__11m3527.dmg&wosid=Mo5ndLZsjioK2DIXcKKGLmyLffK).
4. Download the [lzo native libs](https://github.com/nathanmarz/cascalog-contrib/downloads) and place them in `/opt/local/lib`.

### Configuring Hadoop

You can find more information about Hadoop-LZO [on Cloudera](http://www.cloudera.com/blog/2009/11/hadoop-at-twitter-part-1-splittable-lzo-compression/).

### Building Hadoop-Lzo

This is only necessary if you're trying to rebuild this project.

```bash
git clone https://github.com/kevinweil/hadoop-lzo.git
cd hadoop-lzo
git checkout -b lion 4c5a2270863e0d906e5c3c7cd7a57a7f14436759

JAVA_HOME=$(/usr/libexec/java_home) \
C_INCLUDE_PATH=/opt/local/include LIBRARY_PATH=/opt/local/lib \
CFLAGS="-arch x86_64" ant clean compile-native test tar
```
