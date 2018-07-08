package db

type postgresSession struct {
}

func (ps *postgresSession) Write(wr *pb.WriteRequest) error {
	dblogger.Infof("postgres write called with request:%s", wr)
	return nil
}

func (ps *postgresSession) Close() error {
	dblogger.Infof("postgres close called")
	return nil
}

func newPostgresSession(conf config.SessionConfiger, id string) (Sessioner, error) {
	dblogger.Infof("postgres db session starting")
	return &cockroachSession{}, nil
}
