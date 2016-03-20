# bgpmon

##TODO
- implement modules on server side
	gobgp-link
	prefix-hijack
- implement writers
- thread saftey? does it exist in go? apply with respect to writing (sessions)
- null out defaulted fields (ex "", 0) in gocql insert statements
- implement signaling for writing - don't allow more than 50 go routines actively writing at a time
