#bgpmon

##link gobgpd commands
./bgpmond example/bgpmond_config.toml
./bgpmon open cassandra user pass ip --session_id=CAS1
./bgpmon start gobgpd-link 127.0.0.1 CAS1

sudo -E gobgpd -f gobgp.conf
gobgp global rib add 192.168.0.0/16 -a ipv4

##TODO
- on "run" module need to still be able to "list modules" as they run for awhile

- fix gobgp_link - they removed MonitorBestChanged from the protobuf api
- prefix-hijack module - more verbose activity (just prints out that a hijack occured)
- get common timeuuid for BPGUpdate inserts in cassandra session
- thread saftey? does it exist in go? apply with respect to writing (sessions)
- null out defaulted fields (ex "", 0) in gocql insert statements
- send write errors back to user
