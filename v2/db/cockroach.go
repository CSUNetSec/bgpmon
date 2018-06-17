package db

// values for this are provided by the bgpmond config file.
// then the client can open sessions to it.
type CockroachConfig struct {
	Name     string   // a unique name for this cockroach connection
	CertDir  string   // directory on the bgpmond host containing the certs
	User     string   // user in the DB to run bgpmond as
	Hosts    []string // list of hosts for that cluster
	Database string   // the database under which the bgpmond relations live
}
